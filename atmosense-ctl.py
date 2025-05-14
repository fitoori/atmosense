#!/usr/bin/env python3
"""Cruise‑speed control bridge

Receives authenticated AIRDENS / CRSPEED frames from *airsense_producer.py*,
applies a low‑pass filter to CRSPEED, and drives ArduPilot parameters
``WPNAV_SPEED`` and ``LOIT_SPEED`` in AUTO / GUIDED / LOITER modes. Includes a
15‑s stale‑data fallback that reloads ``/opt/airsense/.env`` and immediately
pushes the last‑known speed. Diagnostics flow via ``STATUSTEXT`` and
``NAMED_VALUE_FLOAT``.

*Version 1.4.1  2025‑05‑14*
————————————————————————
*   **Fail‑fast on missing HMAC key.**
*   **Capture protocol instance** from ``create_datagram_endpoint`` to avoid
    the missing ``get_protocol()`` attribute.
*   **Tasks wrapped with ``create_task``** so they can be cancelled cleanly at
    shutdown.
*   Exponential back‑off in fallback, debug logs for drops / drift, filter
    reset on mode‑change, out‑of‑order packet rejection, zero‑division guard.
"""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import logging
import os
import signal
import socket
import struct
import time
from typing import Dict, Optional, Tuple

from pymavlink import mavutil

# ───────────────────────── configuration ─────────────────────────
RAW_KEY = os.getenv("AIRSENSE_HMAC_KEY")
if not RAW_KEY:
    raise SystemExit("AIRSENSE_HMAC_KEY is not set; aborting")
HMAC_KEY = RAW_KEY.encode()
UDP_PORT = int(os.getenv("AIRSENSE_UDP_PORT", "14560"))
MAV_URI = os.getenv("FLIGHTCTL_MAVLINK_URI", "udp:127.0.0.1:14550")
ALPHA = float(os.getenv("FLIGHTCTL_ALPHA", "0.15"))
MIN_RATIO = float(os.getenv("FLIGHTCTL_MIN_DELTA", "0.02"))
FORCE_SEC = float(os.getenv("FLIGHTCTL_FORCE_SEC", "60"))
SEQ_TIMEOUT = 15.0
ENV_PATH = "/opt/airsense/.env"  # perms 600 recommended

ALLOWED_MODES = {3, 4, 5}  # AUTO, GUIDED, LOITER
MIN_CMS, MAX_CMS = 250, 1200
_VERSION = 1

# ───────────────────────── logging ─────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CRS‑BRG] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
LOG = logging.getLogger(__name__)

# ───────────────── packet helpers ─────────────────
_PKT_STRUCT = struct.Struct("!7sBBf")
_PKT_LEN = _PKT_STRUCT.size + 14

def _parse(pkt: bytes) -> Optional[Tuple[str, float, int]]:
    if len(pkt) != _PKT_LEN:
        LOG.debug("Drop len %d", len(pkt))
        return None
    hdr, mac = pkt[: _PKT_STRUCT.size], pkt[_PKT_STRUCT.size :]
    try:
        tag_b, ver, seq, val = _PKT_STRUCT.unpack(hdr)
    except struct.error:
        LOG.debug("Drop unpack error")
        return None
    if ver != _VERSION:
        LOG.debug("Drop version %d", ver)
        return None
    if not hmac.compare_digest(hmac.new(HMAC_KEY, hdr, hashlib.sha256).digest()[:14], mac):
        LOG.debug("Drop HMAC fail")
        return None
    return tag_b.split(b"\0", 1)[0].decode(errors="ignore"), val, seq

# ───────────────── MAVLink iface ─────────────────
class MavIfc:
    def __init__(self, uri: str):
        LOG.info("Connecting MAVLink → %s", uri)
        try:
            self._mav = mavutil.mavlink_connection(uri, autoreconnect=False)
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(f"MAVLink connect failed: {exc}") from exc
        LOG.info("Waiting heartbeat …")
        try:
            self._mav.wait_heartbeat(timeout=10)
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(f"Heartbeat timeout: {exc}") from exc
        self.mode: Optional[int] = None
        self._t0 = time.time()
        self.controller: Optional["Controller"] = None

    async def pump(self):
        while True:
            msg = self._mav.recv_match(blocking=False)
            if msg and msg.get_type() == "HEARTBEAT":
                prev, self.mode = self.mode, msg.custom_mode
                if prev != self.mode:
                    LOG.info("Mode → %s (%d)", mavutil.mode_string_v10(msg), self.mode)
                    if self.controller:
                        self.controller.reset()
            await asyncio.sleep(0.02)

    # helpers
    def _ms(self):
        return int((time.time() - self._t0) * 1000)

    def send_param(self, name: str, cms: int):
        with contextlib.suppress(Exception):
            self._mav.mav.param_set_send(
                self._mav.target_system,
                self._mav.target_component,
                name.encode(), float(cms),
                mavutil.mavlink.MAV_PARAM_TYPE_REAL32,
            )

    def nv_float(self, name: str, val: float):
        with contextlib.suppress(Exception):
            self._mav.mav.named_value_float_send(self._ms(), name.encode()[:10], float(val))

    def statustext(self, txt: str):
        with contextlib.suppress(Exception):
            self._mav.mav.statustext_send(mavutil.mavlink.MAV_SEVERITY_WARNING, txt[:50].encode())

    def close(self):
        with contextlib.suppress(Exception):
            self._mav.close()

# ───────────────── Controller ─────────────────
class Controller:
    def __init__(self, mav: MavIfc):
        self.mav = mav
        mav.controller = self
        self.v_filtered: Optional[float] = None
        self.last_set: Optional[float] = None
        self.last_ts = 0.0

    def reset(self):
        LOG.debug("Filter reset")
        self.v_filtered = self.last_set = None
        self.last_ts = 0.0

    def ingest(self, v: float):
        self.v_filtered = v if self.v_filtered is None else self.v_filtered + ALPHA * (v - self.v_filtered)
        self.v_filtered = max(MIN_CMS / 100, min(MAX_CMS / 100, self.v_filtered))
        self.mav.nv_float("CRSPEED", self.v_filtered)
        self._maybe_push()

    def force_param_update(self):
        self.last_set = None
        self.last_ts = 0.0
        self._maybe_push()

    def _maybe_push(self):
        if self.v_filtered is None or self.mav.mode not in ALLOWED_MODES:
            return
        now = time.time()
        drift = float("inf") if not self.last_set else abs(self.v_filtered - self.last_set) / max(self.last_set, 1e-6)
        LOG.debug("Drift %.4f", drift)
        if drift < MIN_RATIO and now - self.last_ts < FORCE_SEC:
            return
        if now - self.last_ts < 1.0:
            return
        cms = int(max(MIN_CMS, min(MAX_CMS, self.v_filtered * 100)))
        for p in ("WPNAV_SPEED", "LOIT_SPEED"):
            self.mav.send_param(p, cms)
        self.last_set = self.v_filtered
        self.last_ts = now
        LOG.info("PARAM_SET %d cm/s", cms)

# ───────────────── UDP proto ─────────────────
class AirsenseProto(asyncio.DatagramProtocol):
    def __init__(self, ctrl: Controller):
        self.ctrl = ctrl
        self.last: Dict[str, float] = {}
        self.seq: Dict[str, int] = {}

    def datagram_received(self, data: bytes, addr):
        parsed = _parse(data)
        if not parsed:
            return
        tag, val, seq = parsed
        last_seq = self.seq.get(tag)
        if last_seq is not None and ((seq - last_seq) & 0xFF) in (0, *range(128, 256)):
            return
        self.seq[tag] = seq
        self.last[tag] = time.time()
        if tag == "CRSPEED":
            self.ctrl.ingest(val)
        elif tag == "AIRDENS":
            self.ctrl.mav.nv_float("AIRDENS", val)

# ───────────────── fallback task ─────────────────
async def fallback(proto: AirsenseProto, ctrl: Controller):
    backoff, notified = SEQ_TIMEOUT, False
    while True:
        await asyncio
