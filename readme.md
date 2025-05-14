## Real-Time Air-Density-Aware Cruise-Speed Controller  
Flame Wheel 550 × Navio2 × Raspberry Pi 4B (Copter)

Continuously estimates local **air density ρ** in flight and retunes ArduPilot’s
cruise-speed parameters so that forward-flight power ≈ hover power.  Runs
entirely on the companion computer; the ArduPilot firmware remains stock.

How it works:
	1.	Producer obtains baro pressure + temperature from
MAVLink SCALED_PRESSURE, calculates air density and target cruise speed,
signs two 27-byte UDP frames (AIRDENS and CRSPEED) each second, and writes
“RHO_LAST / V_TARGET_LAST” to /opt/airsense/.env every 60 s.
	2.	Consumer verifies HMAC + sequence, low-pass filters CRSPEED, and, when
change ≥2 % or 60 s elapsed, writes WPNAV_SPEED and LOIT_SPEED via PARAM_SET
(clamped 250–1200 cm s⁻¹).  If no CRSPEED arrives for 15 s it loads the
last .env value, pushes it, and warns the operator.

---
## Quick Install

sudo apt update
sudo apt install python3-pip python3-venv git make
git clone https://github.com/fitoori/atmosense.git
cd atmosense
python3 -m venv venv && . venv/bin/activate
pip3 install -r requirements.txt
export AIRSENSE_HMAC_KEY=$(openssl rand -hex 16)
sudo make deploy
sudo systemctl status airsense.service ctrlbridge.service

---

## Features
* **Density-aware speed set-point** — recalculates `WPNAV_SPEED` / `LOIT_SPEED`
  once per second (default) so the multirotor flies at its power sweet-spot.
* **Signed telemetry link** — 27-byte UDP frames with truncated HMAC-SHA-256.
* **Low-pass smoothing & rate clamp** — prevents parameter chatter; ≤ 1 Hz
  writes, 2 % dead-band.
* **Fail-safe fallback** — if sensor data stalls for 15 s, reloads last speed
  from `/opt/airsense/.env`, pushes it, and warns the pilot.
* **Systemd-native** — self-restarting services, clean SIGTERM/SIGINT shutdown.
* **Unit-tested** — ≥ 90 % coverage on math, framing, filtering, fallback paths.

---

## Directory Layout
atmosense/                     # project root
├─ atmosense-calc.py  # producer (Airsense)
├─ atmosense-ctl.py       # consumer (CtrlBridge)
├─ systemd/
│   ├─ atmosense-calc.service
│   └─ atmosense-ctl.service
└─  tests/                     # pytest suite

---
Key environment variables

AIRSENSE_HMAC_KEY     32-char hex, shared by both services
AIRSENSE_RATE          1        sensor sample rate (Hz)
AIRSENSE_UDP_PORT   14560       producer → consumer port
FLIGHTCTL_ALPHA      0.15       low-pass weighting factor
FLIGHTCTL_FORCE_SEC     60      unconditional parameter refresh period

---

Default config assumes:
	•	ArduPilot MAVLink on udp:127.0.0.1:14550
	•	Companion UDP loopback port 14560 for Airsense frames.

Override via environment variables in the .service files.

