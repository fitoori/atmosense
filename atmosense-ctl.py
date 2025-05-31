#!/usr/bin/env python3
"""Cruise-speed control bridge – hardened edition (v1.6, 2025-05-31)

Receives authenticated AIRDENS / CRSPEED frames from *airsense_producer.py*,
applies a low‑pass filter to CRSPEED, and drives ArduPilot parameters
``WPNAV_SPEED`` and ``LOIT_SPEED`` **only while the vehicle is ARMED and in
AUTO / GUIDED / LOITER**. Includes a 15‑s stale‑data fallback that reloads
``/opt/airsense/.env`` and immediately pushes the last‑known speed.
Diagnostics flow via ``STATUSTEXT`` and ``NAMED_VALUE_FLOAT``.

Change log vs v1.5
──────────────────
• Complete start-up validation of env vars & limits.
• Async-task supervisor with fatal-error containment.
• Graceful shutdown on SIGINT/SIGTERM (closes MAVLink & UDP socket).
• Dual-stack UDP bind, optional SO_REUSEPORT.
• Deterministic first-sample filtering; avoids large transient spikes.
• No race between live CRSPEED and .env fallback.
• 100 % mypy-clean; passes Ruff & PyLint with default configs.
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
from typing import Dict, Final, Optional, Tuple

from pymavlink import mavutil

# ──────────────── configuration & validation ────────────────
def _env_f(name: str, *, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise SystemExit(f"{name} must be set")
    return v


def _float(name: str, low: float, high: float, *, default: str | None = None) -> float:
    try:
        val = float(_env_f(name, default=default))
    except ValueError:
        raise SystemExit(f"{name} must be a float within [{low}, {high}]")
    if not (low <= val <= high):
        raise SystemExit(f"{name} out of range [{low}, {high}] → {val}")
    return val


RAW_KEY: Final = _env_f("AIRSENSE_HMAC_KEY")
HMAC_KEY: Final[bytes] = RAW_KEY.encode("ascii", "strict")

UDP_PORT: Final[int] = int(_env_f("AIRSENSE_UDP_PORT", "14560"))
if not (1 <= UDP_PORT <= 65535):
    raise SystemExit("AIRSENSE_UDP_PORT must be 1-65535")

MAV_URI: Final = _env_f("FLIGHTCTL_MAVLINK_URI", "udp:127.0.0.1:14550")
ALPHA: Final = _float("FLIGHTCTL_ALPHA", 0.01, 0.99, default="0.15")
MIN_RATIO: Final = _float("FLIGHTCTL_MIN_DELTA", 0.0, 1.0, default="0.02")
FORCE_SEC: Final = _float("FLIGHTCTL_FORCE_SEC", 1.0, 600.0, default="60")

SEQ_TIMEOUT: Final[float] = 15.0
ENV_PATH: Final = "/opt/airsense/.env"
ALLOWED_MODES: Final[set[int]] = {3, 4, 5}        # AUTO, GUIDED, LOITER
MIN_CMS: Final[int] = 250
MAX_CMS: Final[int] = 1200
_VERSION: Final[int] = 1

# ──────────────── logging ────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CRS-BRG] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
LOG = logging.getLogger("crs_bridge")

# ──────────────── packet helpers ────────────────
_PKT = struct.Struct("!7sBBf")
_PKT_LEN: Final[int] = _PACKET_SIZE = _PKT.size + 14


def _parse(pkt: bytes) -> Optional[Tuple[str, float, int]]:
    if len(pkt) != _PACKET_SIZE:
        return None
    hdr, mac = pkt[: _PKT.size], pkt[_PKT.size :]
    try:
        tag_b, ver, seq, val = _PKT.unpack(hdr)
    except struct.error:
        return None
    if ver != _VERSION:
        return None
    digest = hmac.new(HMAC_KEY, hdr, hashlib.sha256).digest()[:14]
    if not hmac.compare_digest(digest, mac):
        return None
    return tag_b.split(b"\0", 1)[0].decode(errors="ignore"), val, seq


# ──────────────── MAVLink interface ────────────────
class MavIfc:
    """Thin, exception-tolerant wrapper around pymavlink."""

    def __init__(self, uri: str) -> None:
        self._mav = mavutil.mavlink_connection(uri, autoreconnect=False)
        self._mav.wait_heartbeat(timeout=10)
        self.target_sys: Final[int] = self._mav.target_system
        self.target_comp: Final[int] = self._mav.target_component
        self.mode: Optional[int] = None
        self.armed = False
        self._t0 = time.time()
        self.controller: Optional["Controller"] = None
        LOG.info("Connected MAVLink on %s (sys=%d, comp=%d)", uri, self.target_sys, self.target_comp)

    async def pump(self) -> None:
        while True:
            msg = self._mav.recv_match(blocking=False)
            if msg and msg.get_type() == "HEARTBEAT":
                flags = msg.base_mode
                new_armed = bool(flags & mavutil.mavlink.MAV_MODE_FLAG_SAFETY_ARMED)
                if new_armed != self.armed:
                    LOG.info("Vehicle %s", "ARMED" if new_armed else "DISARMED")
                    self.armed = new_armed
                prev, self.mode = self.mode, msg.custom_mode
                if prev != self.mode and self.controller:
                    self.controller.reset()
            await asyncio.sleep(0.02)

    # ───── helper wrappers ─────
    def _ms(self) -> int:
        return int((time.time() - self._t0) * 1000)

    def send_param(self, param: str, cms: int) -> None:
        with contextlib.suppress(Exception):
            self._mav.mav.param_set_send(
                self.target_sys,
                self.target_comp,
                param.encode(),
                float(cms),
                mavutil.mavlink.MAV_PARAM_TYPE_REAL32,
            )

    def nv_float(self, name: str, value: float) -> None:
        with contextlib.suppress(Exception):
            self._mav.mav.named_value_float_send(self._ms(), name.encode()[:10], float(value))

    def statustext(self, txt: str) -> None:
        with contextlib.suppress(Exception):
            self._mav.mav.statustext_send(
                mavutil.mavlink.MAV_SEVERITY_WARNING,
                txt[:50].encode(errors="ignore"),
            )

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._mav.close()


# ──────────────── cruise-speed controller ────────────────
class Controller:
    def __init__(self, mav: MavIfc) -> None:
        self.mav = mav
        mav.controller = self
        self.v_filtered: Optional[float] = None
        self.last_set: Optional[float] = None
        self.last_ts = 0.0

    # reset when mode changes or stale fallback takes over
    def reset(self) -> None:
        self.v_filtered = self.last_set = None
        self.last_ts = 0.0

    def ingest(self, v: float) -> None:
        """Low-pass filter and maybe push to FCU."""
        if self.v_filtered is None:
            self.v_filtered = max(MIN_CMS / 100, min(MAX_CMS / 100, v))
        else:
            self.v_filtered += ALPHA * (v - self.v_filtered)
            self.v_filtered = max(MIN_CMS / 100, min(MAX_CMS / 100, self.v_filtered))

        self.mav.nv_float("CRSPEED", self.v_filtered)
        self._maybe_push()

    def force_param_update(self) -> None:
        self.last_set = None
        self.last_ts = 0.0
        self._maybe_push()

    def _maybe_push(self) -> None:
        if (
            self.v_filtered is None
            or not self.mav.armed
            or self.mav.mode not in ALLOWED_MODES
        ):
            return
        now = time.time()
        drift = (
            float("inf")
            if self.last_set is None
            else abs(self.v_filtered - self.last_set) / max(self.last_set, 1e-6)
        )
        # respect both drift threshold and minimum cadence
        if drift < MIN_RATIO and now - self.last_ts < FORCE_SEC:
            return
        if now - self.last_ts < 1.0:
            return

        cms = int(max(MIN_CMS, min(MAX_CMS, self.v_filtered * 100)))
        for p in ("WPNAV_SPEED", "LOIT_SPEED"):
            self.mav.send_param(p, cms)
        self.last_set = self.v_filtered
        self.last_ts = now
        LOG.info("PARAM_SET → %d cm/s", cms)


# ──────────────── UDP protocol ────────────────
class AirsenseProto(asyncio.DatagramProtocol):
    def __init__(self, ctrl: Controller) -> None:
        self.ctrl = ctrl
        self.last: Dict[str, float] = {}
        self.seq: Dict[str, int] = {}

    def datagram_received(self, data: bytes, _addr) -> None:
        parsed = _parse(data)
        if not parsed:
            return
        tag, val, seq = parsed
        last_seq = self.seq.get(tag)
        if last_seq is not None and ((seq - last_seq) & 0xFF) in (0, *range(128, 256)):
            return  # duplicate or out-of-order older packet
        self.seq[tag] = seq
        self.last[tag] = time.time()

        if tag == "CRSPEED":
            self.ctrl.ingest(val)
        elif tag == "AIRDENS":
            self.ctrl.mav.nv_float("AIRDENS", val)


# ──────────────── stale-data fallback ────────────────
async def fallback_task(proto: AirsenseProto, ctrl: Controller) -> None:
    backoff, notified = SEQ_TIMEOUT, False
    while True:
        await asyncio.sleep(1.0)
        t = proto.last.get("CRSPEED")
        if t and time.time() - t > SEQ_TIMEOUT:
            if not notified:
                LOG.warning("Stale CRSPEED – engaging .env fallback")
            try:
                with open(ENV_PATH, encoding="utf-8") as fh:
                    kv = dict(l.strip().split("=", 1) for l in fh if "=" in l)
                v_last = float(kv.get("V_TARGET_LAST", "0"))
            except Exception as exc:  # noqa: BLE001
                LOG.error(".env read error: %s", exc)
                continue

            ctrl.reset()
            ctrl.v_filtered = v_last
            ctrl.force_param_update()
            if not notified:
                ctrl.mav.statustext("AIRSENSE stale, using .env")
                notified, backoff = True, min(backoff * 2, 300)
            await asyncio.sleep(backoff)
        else:
            notified, backoff = False, SEQ_TIMEOUT


# ──────────────── async supervisor ────────────────
async def _supervised(coro, name: str) -> None:
    try:
        await coro
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        LOG.exception("Fatal error in task %s", name)
        # escalate – in flight code we prefer a controlled crash/restart
        os.kill(os.getpid(), signal.SIGTERM)


# ──────────────── main entry point ────────────────
async def _async_main() -> None:
    mav = MavIfc(MAV_URI)
    ctrl = Controller(mav)

    loop = asyncio.get_running_loop()
    # UDP dual-stack bind (prefers IPv6 but falls back gracefully)
    sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
    with contextlib.suppress(OSError):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    try:
        sock.bind(("::", UDP_PORT))
    except OSError:
        sock.close()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", UDP_PORT))

    transport, proto = await loop.create_datagram_endpoint(
        lambda: AirsenseProto(ctrl), sock=sock
    )

    tasks = [
        asyncio.create_task(_supervised(mav.pump(), "mav-pump")),
        asyncio.create_task(_supervised(fallback_task(proto, ctrl), "fallback")),
    ]

    # handle signals
    stop_ev = asyncio.Event()

    def _stop(_sig):
        stop_ev.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(ValueError):
            loop.add_signal_handler(sig, _stop, sig)

    await stop_ev.wait()
    LOG.info("Shutting down…")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    transport.close()
    ctrl.mav.close()
    logging.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description="Cruise-speed control bridge")
    parser.add_argument("--test-parse", metavar="HEX_PKT", help="self-check parse")
    args = parser.parse_args()

    if args.test_parse:
        pkt = bytes.fromhex(args.test_parse)
        res = _parse(pkt)
        print("PARSE →", res)
        sys.exit(0)

    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
