#!/usr/bin/env python3
"""
Airsense producer – v1.2  (2025-05-31)

Pulls pressure / temperature (and optionally humidity)
from ArduPilot via MAVLink, computes dry‑air density and a power‑parity
cruise speed, broadcasts two authenticated 27‑byte UDP frames (AIRDENS /
CRSPEED) each cycle, and writes /opt/airsense/.env every 60 s for crash
recovery.

Wire format, math, and interaction with the cruise-speed bridge are unchanged
from v1.1; only robustness improvements were made.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import hmac
import ipaddress
import logging
import os
import signal
import socket
import struct
import sys
import time
from typing import Final, Optional, Tuple

from pymavlink import mavutil

# ─────────────── configuration & hard checks ───────────────
def _env(name: str, *, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise SystemExit(f"{name} must be set")
    return v


def _f(name: str, lo: float, hi: float, *, default: str | None = None) -> float:
    try:
        val = float(_env(name, default=default))
    except ValueError:
        raise SystemExit(f"{name} must be a float")
    if not lo <= val <= hi:
        raise SystemExit(f"{name}={val} out of [{lo}, {hi}]")
    return val


RAW_KEY: Final[str] = _env("AIRSENSE_HMAC_KEY")
HMAC_KEY: Final[bytes] = RAW_KEY.encode("ascii", "strict")

UDP_TARGET_STR: Final[str] = _env("AIRSENSE_UDP_TARGET", "127.0.0.1")
try:
    ipaddress.ip_address(UDP_TARGET_STR)  # allow literal IP or hostname
except ValueError:
    try:
        socket.getaddrinfo(UDP_TARGET_STR, None)
    except socket.gaierror:
        raise SystemExit(f"AIRSENSE_UDP_TARGET cannot be resolved: {UDP_TARGET_STR}")

UDP_PORT: Final[int] = int(_env("AIRSENSE_UDP_PORT", "14560"))
if not 1 <= UDP_PORT <= 65535:
    raise SystemExit("AIRSENSE_UDP_PORT must be 1-65535")

MAV_URI: Final[str] = _env("AIRSENSE_MAVLINK_URI", "udp:127.0.0.1:14550")
SAMPLE_HZ: Final[float] = _f("AIRSENSE_RATE", 0.2, 50.0, default="1")
MSG_RATE_HZ: Final[float] = _f("AIRSENSE_MAV_MSG_RATE", 1.0, 50.0, default="10")
ENV_PATH: Final[str] = "/opt/airsense/.env"

# flight constants
R_D: Final[float] = 287.058      # J kg-1 K-1
RHO_REF: Final[float] = 1.225    # kg m-3
V_REF: Final[float] = 10.0       # m s-1
_VERSION: Final[int] = 1

# ─────────────── logging ───────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [AIRSENSE] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
LOG = logging.getLogger("airsense")

# ─────────────── packet helpers ───────────────
_PKT = struct.Struct("!7sBBf")          # tag, ver, seq, float
_PKT_LEN: Final[int] = _PKT.size + 14   # =27


def _build(tag: str, value: float, seq: int) -> bytes:
    hdr = _PKT.pack(tag.encode()[:7].ljust(7, b"\0"), _VERSION, seq & 0xFF, value)
    mac = hmac.new(HMAC_KEY, hdr, hashlib.sha256).digest()[:14]
    return hdr + mac


# ─────────────── physics helpers ───────────────
def _rho(p_pa: float, t_c: float) -> float:
    return p_pa / (R_D * (t_c + 273.15))


def _v_target(rho: float) -> float:
    return V_REF * (RHO_REF / rho) ** 0.5 if rho > 0 else V_REF


# ─────────────── atomic .env writer ───────────────
def _write_env(rho_val: float, v_val: float) -> None:
    tmp = ENV_PATH + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(f"RHO_LAST={rho_val:.4f}\nV_TARGET_LAST={v_val:.4f}\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, ENV_PATH)  # atomic
    except Exception as exc:       # noqa: BLE001
        LOG.error(".env write failed: %s", exc)


# ─────────────── MAV feed class ───────────────
class MavFeed:
    """Caches latest pressure (Pa) and temperature (°C)."""

    def __init__(self, uri: str) -> None:
        try:
            self._mav = mavutil.mavlink_connection(uri)
        except Exception as exc:    # noqa: BLE001
            LOG.critical("MAVLink connect failed: %s", exc)
            raise SystemExit(1) from exc

        LOG.info("Waiting for heartbeat …")
        self._mav.wait_heartbeat()
        LOG.info("Heartbeat OK – requesting %.1f Hz data", MSG_RATE_HZ)

        for mid in (
            mavutil.mavlink.MAVLINK_MSG_ID_SCALED_PRESSURE,
            mavutil.mavlink.MAVLINK_MSG_ID_HYGROMETER_SENSOR,
        ):
            with contextlib.suppress(Exception):
                self._mav.mav.command_long_send(
                    self._mav.target_system,
                    self._mav.target_component,
                    mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL,
                    0,
                    mid,
                    int(1e6 / MSG_RATE_HZ),
                    0,
                    0,
                    0,
                    0,
                    0,
                )

        self.press_pa: Optional[float] = None
        self.temp_c: Optional[float] = None
        self.hum_pct: Optional[float] = None

        self._loop = asyncio.get_running_loop()
        self._using_reader = False
        fd = getattr(self._mav, "fd", None)
        if fd is not None:
            try:
                self._loop.add_reader(fd, self._on_data)
                self._using_reader = True
            except (RuntimeError, NotImplementedError):
                pass

        if not self._using_reader:
            LOG.info("FD hook unavailable – polling at 100 Hz")
            self._poll_task = self._loop.create_task(self._poll())

    # —— MAV message handlers ——
    def _handle(self, msg) -> None:  # type: ignore[override]
        t = msg.get_type()
        if t == "SCALED_PRESSURE":
            self.press_pa = msg.press_abs * 100.0
            self.temp_c = msg.temperature / 100.0
        elif t == "HYGROMETER_SENSOR":
            self.hum_pct = msg.humidity

    # —— event-driven mode ——
    def _on_data(self) -> None:
        try:
            while (m := self._mav.recv_match(blocking=False)) is not None:
                self._handle(m)
        except Exception as exc:  # noqa: BLE001
            LOG.error("MAV recv error: %s", exc)

    # —— poll-mode ——
    async def _poll(self) -> None:      # pragma: no cover
        while True:
            self._on_data()
            await asyncio.sleep(0.01)

    async def close(self) -> None:
        if not self._using_reader:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
        with contextlib.suppress(Exception):
            self._mav.close()


# ─────────────── producer coroutine ───────────────
async def _produce(feed: MavFeed, stop_ev: asyncio.Event) -> None:
    # UDP connect with dual-stack fallback
    sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
    with contextlib.suppress(OSError):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    try:
        sock.connect((UDP_TARGET_STR, UDP_PORT))
    except OSError:
        sock.close()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect((UDP_TARGET_STR, UDP_PORT))

    seq = 0
    last_env = time.time()
    period = 1.0 / SAMPLE_HZ

    while not stop_ev.is_set():
        t0 = time.time()

        if feed.press_pa is not None and feed.temp_c is not None:
            rho_val = _rho(feed.press_pa, feed.temp_c)
            v_val = _v_target(rho_val)

            try:
                sock.send(_build("AIRDENS", rho_val, seq))
                sock.send(_build("CRSPEED", v_val, seq))
            except OSError as exc:
                LOG.error("UDP send failed: %s", exc)

            if time.time() - last_env >= 60.0:
                _write_env(rho_val, v_val)
                last_env = time.time()

            seq = (seq + 1) & 0xFF
        else:
            LOG.debug("Awaiting first sensor sample…")

        # drift-free sleep
        dt = time.time() - t0
        if dt > period * 1.1:
            LOG.warning("Cycle overrun: %.1f ms", dt * 1000)
        await asyncio.wait_for(stop_ev.wait(), timeout=max(period - dt, 0.0))


# ─────────────── async supervisor wrapper ───────────────
async def _supervised(coro, name: str) -> None:
    try:
        await coro
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        LOG.exception("Fatal error in task %s", name)
        os.kill(os.getpid(), signal.SIGTERM)


# ─────────────── entry point ───────────────
async def _async_main() -> None:
    stop_ev = asyncio.Event()

    def _sig(_s, _f):
        stop_ev.set()

    for s in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(ValueError):
            signal.signal(s, _sig)

    feed = MavFeed(MAV_URI)
    prod_task = asyncio.create_task(_supervised(_produce(feed, stop_ev), "producer"))

    await stop_ev.wait()
    LOG.info("Graceful shutdown requested")
    prod_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await prod_task
    await feed.close()
    # One last .env write if possible
    if feed.press_pa and feed.temp_c:
        _write_env(_rho(feed.press_pa, feed.temp_c), _v_target(_rho(feed.press_pa, feed.temp_c)))
    LOG.info("Shutdown complete")


def main() -> None:
    ap = argparse.ArgumentParser(description="Airsense producer")
    ap.add_argument("--test-build", action="store_true", help="emit a sample hex packet and exit")
    args = ap.parse_args()

    if args.test_build:
        pkt = _build("AIRDENS", 1.2345, 42)
        print(pkt.hex())
        sys.exit(0)

    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
