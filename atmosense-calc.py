#!/usr/bin/env python3
"""
Airsense producer – pulls pressure / temperature (and optionally humidity)
from ArduPilot via MAVLink, computes dry‑air density and a power‑parity
cruise speed, broadcasts two authenticated 27‑byte UDP frames (AIRDENS /
CRSPEED) each cycle, and writes /opt/airsense/.env every 60 s for crash
recovery.

2025‑05‑13 (v1.1)
  • Event‑driven MAVLink ingest (loop.add_reader) with polling fallback.
  • Graceful shutdown on SIGINT/SIGTERM with final .env sync.
  • Connected UDP socket (cheaper & immediate ICMP feedback).
  • Humidity units clarified (MAVLink already %RH).
  • Belt‑and‑suspenders error handling everywhere.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import signal
import socket
import struct
import time
from typing import Optional

from pymavlink import mavutil

# ───────────── configuration – override via env or systemd override ───────────
HMAC_KEY   = os.getenv("AIRSENSE_HMAC_KEY", "CHANGEME_UNSAFE_KEY").encode()
MAV_URI    = os.getenv("AIRSENSE_MAVLINK_URI", "udp:127.0.0.1:14550")
UDP_TARGET = os.getenv("AIRSENSE_UDP_TARGET", "127.0.0.1")
UDP_PORT   = int(os.getenv("AIRSENSE_UDP_PORT", 14560))
SAMPLE_HZ  = float(os.getenv("AIRSENSE_RATE", "1"))         # producer rate
MSG_RATE_HZ = float(os.getenv("AIRSENSE_MAV_MSG_RATE", "10")) # SCALED_PRESSURE req
ENV_PATH   = "/opt/airsense/.env"

# aerodynamic constants
R_D      = 287.058   # J·kg‑1·K‑1 (specific gas constant for dry air)
RHO_REF  = 1.225     # kg·m‑3 (ISA sea‑level)
V_REF    = 10.0      # m·s‑1 reference cruise speed

VERSION  = 1         # frame version byte

# ──────────────────────────────── logging ─────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [AIRSENSE] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("airsense")

# ───────────────────────────── helper functions ───────────────────────────────
# Pre‑compile struct to save a few µs on the Pi 4.
_PKT_STRUCT = struct.Struct("!7sBBf")  # tag, ver, seq, value


def compute_rho(pressure_pa: float, temp_c: float, *_unused) -> float:
    """Dry‑air density (kg m‑3). Humidity correction ≤ 2 % is ignored."""
    return pressure_pa / (R_D * (temp_c + 273.15))


def compute_v_target(rho: float) -> float:
    """Power‑parity cruise speed vs hover (m s‑1)."""
    return V_REF * (RHO_REF / rho) ** 0.5 if rho > 0 else V_REF


def build_pkt(tag: str, value: float, seq: int) -> bytes:
    """Return authenticated 27‑byte frame: 7s BB f 14B."""
    hdr = _PKT_STRUCT.pack(tag.encode()[:7].ljust(7, b"\0"), VERSION, seq & 0xFF, value)
    mac = hmac.new(HMAC_KEY, hdr, hashlib.sha256).digest()[:14]
    return hdr + mac


def atomic_write_env(rho: float, v: float) -> None:
    """Best‑effort crash‑proof write of the recovery file."""
    tmp = ENV_PATH + ".tmp"
    try:
        with open(tmp, "w", encoding="utf‑8") as f:
            f.write(f"RHO_LAST={rho:.4f}\nV_TARGET_LAST={v:.4f}\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, ENV_PATH)       # atomic on same fs
    except Exception as e:              # noqa: BLE001
        logger.error(".env write failed: %s", e)
        # tmp may remain – ignore

# ───────────────────────────── MAVLink feed class ─────────────────────────────
class MavFeed:
    """Caches latest P (Pa), T (°C), H (% RH) from ArduPilot."""

    def __init__(self, uri: str, rate_hz: float) -> None:
        try:
            self._mav = mavutil.mavlink_connection(uri)
        except Exception as e:          # noqa: BLE001
            logger.critical("MAVLink connection failed (%s): %s", uri, e)
            raise SystemExit(1) from e

        logger.info("Waiting for ArduPilot heartbeat …")
        try:
            self._mav.wait_heartbeat()
        except Exception as e:          # noqa: BLE001
            logger.critical("Heartbeat timeout: %s", e)
            raise SystemExit(2) from e
        logger.info("Heartbeat OK – requesting SCALED_PRESSURE @ %.1f Hz", rate_hz)

        # Request streams (ignore ACKs – ArduPilot accepts silently).
        for msg_id in (mavutil.mavlink.MAVLINK_MSG_ID_SCALED_PRESSURE,
                       mavutil.mavlink.MAVLINK_MSG_ID_HYGROMETER_SENSOR):
            try:
                self._mav.mav.command_long_send(
                    self._mav.target_system,
                    self._mav.target_component,
                    mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL,
                    0,
                    msg_id,
                    int(1e6 / rate_hz), 0, 0, 0, 0, 0,
                )
            except Exception as e:      # noqa: BLE001
                logger.warning("Set‑message‑interval failed for id %d: %s", msg_id, e)

        self.press_pa: Optional[float] = None
        self.temp_c : Optional[float] = None
        self.hum_pct: Optional[float] = None  # not yet used in rho

        # Event‑driven reader registration.
        loop = asyncio.get_running_loop()
        self._using_reader = False
        fd = getattr(self._mav, "fd", None)
        if fd is not None:
            try:
                loop.add_reader(fd, self._on_data)
                self._using_reader = True
                logger.debug("MAVLink fd %s hooked into event loop", fd)
            except (NotImplementedError, RuntimeError):
                pass  # fall back to polling

        if not self._using_reader:
            logger.info("Falling back to 100 Hz poll loop (no FD support)")
            self._poll_task = loop.create_task(self._poll())

    # ——— event‑mode ———
    def _on_data(self) -> None:
        try:
            while True:
                msg = self._mav.recv_match(blocking=False)
                if msg is None:
                    break
                self._handle_msg(msg)
        except Exception as e:          # noqa: BLE001
            logger.error("MAVLink recv error: %s", e)

    # ——— poll‑mode ———
    async def _poll(self) -> None:      # pragma: no cover (timing)
        while True:
            self._on_data()
            await asyncio.sleep(0.01)

    # ——— message parser ———
    def _handle_msg(self, msg):  # type: ignore[override]
        if msg.get_type() == "SCALED_PRESSURE":
            self.press_pa = msg.press_abs * 100.0  # hPa → Pa
            self.temp_c   = msg.temperature / 100.0
        elif msg.get_type() == "HYGROMETER_SENSOR":
            # Field is already %RH (0–100).
            self.hum_pct  = msg.humidity

    async def close(self) -> None:
        if not self._using_reader:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
        try:
            self._mav.close()
        except Exception:
            pass

# ───────────────────────────── main producer loop ─────────────────────────────
async def producer(feed: MavFeed, stop: asyncio.Event) -> None:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(False)
        try:
            sock.connect((UDP_TARGET, UDP_PORT))
        except OSError as e:
            logger.warning("Cannot connect UDP target %s:%d – will use sendto(): %s",
                           UDP_TARGET, UDP_PORT, e)
        seq, last_env_ts = 0, 0.0
        while not stop.is
