#!/usr/bin/env python3
"""Cruise‑speed control bridge

Receives authenticated AIRDENS / CRSPEED frames from *airsense_producer.py*,
applies a low‑pass filter to CRSPEED, and drives ArduPilot parameters
``WPNAV_SPEED`` and ``LOIT_SPEED`` **only while the vehicle is ARMED and in
AUTO / GUIDED / LOITER**. Includes a 15‑s stale‑data fallback that reloads
``/opt/airsense/.env`` and immediately pushes the last‑known speed.
Diagnostics flow via ``STATUSTEXT`` and ``NAMED_VALUE_FLOAT``.

*Version 1.5  2025‑05‑14*
—————————————————————
*   Adds **arm‑status gating** – no PARAM_SET until the safety flag is set.
*   Debug logs for armed→disarmed transitions.
*   Minor clean‑ups from 1.4.1 retained.
"""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import logging
import os
import signal
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
MAV_URI  = os.getenv("FLIGHTCTL_MAVLINK_URI", "udp:127.0.0.1:14550")
ALPHA    = float(os.getenv("FLIGHTCTL_ALPHA", "0.15"))
MIN_RATIO= float(os.getenv("FLIGHTCTL_MIN_DELTA", "0.02"))
FORCE_SEC= float(os.getenv("FLIGHTCTL_FORCE_SEC", "60"))
SEQ_TIMEOUT = 15.0
ENV_PATH = "/opt/airsense/.env"

ALLOWED_MODES = {3, 4, 5}  # AUTO, GUIDED, LOITER
MIN_CMS, MAX_CMS = 250, 1200
_VERSION = 1

# ───────────────────────── logging ─────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CRS-BRG] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
LOG = logging.getLogger(__name__)

# ───────────────── packet helpers ─────────────────
_PKT = struct.Struct("!7sBBf")
_PKT_LEN = _PKT.size + 14

def _parse(pkt: bytes) -> Optional[Tuple[str, float, int]]:
    if len(pkt) != _PKT_LEN:
        return None
    hdr, mac = pkt[:_PKT.size], pkt[_PKT.size:]
    try:
        tag_b, ver, seq, val = _PKT.unpack(hdr)
    except struct.error:
        return None
    if ver != _VERSION:
        return None
    if not hmac.compare_digest(hmac.new(HMAC_KEY, hdr, hashlib.sha256).digest()[:14], mac):
        return None
    return tag_b.split(b"\0", 1)[0].decode(errors="ignore"), val, seq

# ───────────────── MAVLink iface ─────────────────
class MavIfc:
    def __init__(self, uri: str):
        self._mav = mavutil.mavlink_connection(uri, autoreconnect=False)
        self._mav.wait_heartbeat(timeout=10)
        self.mode: Optional[int] = None
        self.armed = False
        self._t0 = time.time()
        self.controller: Optional["Controller"] = None

    async def pump(self):
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

    # helpers
    def _ms(self):
        return int((time.time() - self._t0) * 1000)

    def send_param(self, p: str, cms: int):
        with contextlib.suppress(Exception):
            self._mav.mav.param_set_send(self._mav.target_system, self._mav.target_component,
                                         p.encode(), float(cms), mavutil.mavlink.MAV_PARAM_TYPE_REAL32)

    def nv_float(self, n: str, v: float):
        with contextlib.suppress(Exception):
            self._mav.mav.named_value_float_send(self._ms(), n.encode()[:10], float(v))

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
        self.v_filtered = self.last_set = None
        self.last_ts = 0.0

    def ingest(self, v: float):
        self.v_filtered = v if self.v_filtered is None else self.v_filtered + ALPHA * (v - self.v_filtered)
        self.v_filtered = max(MIN_CMS/100, min(MAX_CMS/100, self.v_filtered))
        self.mav.nv_float("CRSPEED", self.v_filtered)
        self._maybe_push()

    def force_param_update(self):
        self.last_set = None
        self.last_ts = 0.0
        self._maybe_push()

    def _maybe_push(self):
        if (self.v_filtered is None or not self.mav.armed or
                self.mav.mode not in ALLOWED_MODES):
            return
        now = time.time()
        drift = float('inf') if not self.last_set else abs(self.v_filtered - self.last_set) / max(self.last_set, 1e-6)
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

# ───────────────── UDP proto ─────────────────
class AirsenseProto(asyncio.DatagramProtocol):
    def __init__(self, ctrl: Controller):
        self.ctrl = ctrl
        self.last: Dict[str, float] = {}
        self.seq: Dict[str, int] = {}

    def datagram_received(self, data: bytes, addr):
        p = _parse(data)
        if not p:
            return
        tag, val, seq = p
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
        await asyncio.sleep(1)
        t = proto.last.get("CRSPEED")
        if t and time.time() - t > SEQ_TIMEOUT:
            if not notified:
                LOG.warning("Stale CRSPEED – fallback engaged")
            try:
                with open(ENV_PATH, "r", encoding="utf-8") as f:
                    kv = dict(line.strip().split("=", 1) for line in f if "=" in line)
                v_last = float(kv.get("V_TARGET_LAST", "0"))
            except Exception as exc:
                LOG.error(".env read error: %s", exc)
                continue
            ctrl.reset()
            ctrl.v_filtered = v_last
            ctrl.force_param_update()
            if not notified:
                ctrl.mav.statustext("AIRSENSE stale, using .env")
                notified, backoff = True, min(backoff*2, 300)
            await asyncio.sleep(backoff)
        else:
            notified, backoff = False, SEQ_TIMEOUT

# ───────────────── main ─────────────────
async def main():
    mav = MavIfc(MAV_URI)
    ctrl = Controller(mav)
    loop = asyncio.get_running_loop()

    transport, proto = await loop.create

