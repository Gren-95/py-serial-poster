# py-serial-poster

Lightweight Windows utility that reads values from a serial (COM) port,
deduplicates recent scans using a log file, and POSTs accepted values to an HTTP endpoint.

Features
- Log-based global de-duplication (configurable time window).
- Optional system tray UI with port selector and status indicators.
- Controls for asserting DTR/RTS lines (useful for some devices).
- Configurable length and prefix filters for incoming values.
- Packaged EXE via PyInstaller (GitHub Actions uploads ZIP release).

Quick start

1. Create a virtualenv and install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. Run locally (console mode):

```powershell
python scanner.py --port COM5 --baud 9600
```

Common CLI flags
- `--value-len` — expected value length (default 12). Use `0` to disable length checks.
- `--allowed-prefixes` — comma-separated allowed prefixes (default: `126,226`).
- `--dtr` / `--rts` — assert control lines after opening port.
- `--dump-serial` — dump raw serial HEX for debugging.
- `--no-tray` — run without the system tray UI (useful for servers).

Logs and dedupe files
- Application log: `C:\temp\scanner\app.log` (daily rotated).
- Dedupe records: `C:\temp\scanner\<ISOYEAR>W<ISOWEEK>\YYYY-MM-DD.log`.

Build (no console window)

To produce a single-file Windows EXE with no console window (windowed):

```powershell
python -m PyInstaller --clean --noconfirm --onefile --noconsole --name scanner scanner.py
```

Notes on CI
- The repository includes a GitHub Actions workflow that builds the EXE and
	uploads a ZIP artifact to a GitHub Release when you push a `v*` tag.

If you want me to build and test the EXE locally on this machine, say so and
I'll download the release asset and verify it runs headless.

