from __future__ import annotations

import argparse
import logging
import os
import queue
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import requests
import serial
from serial.tools import list_ports

import pystray
from PIL import Image, ImageDraw

stop_event = threading.Event()

# Used to request a live serial port switch without restarting the app.
port_change_event = threading.Event()

status_lock = threading.Lock()
status = {
    "state": "Starting...",
    "connected": False,
    "last_value": "",
    "last_post": "",
    "last_error": "",
    "posts_ok": 0,
    "posts_fail": 0,
}


@dataclass
class Config:
    port: str
    baud: int
    read_timeout: float
    scan_hz_cap: float
    dedupe_window_sec: int
    endpoint: str
    header_name: Optional[str]
    header_value: Optional[str]
    verify_tls: bool
    post_timeout_sec: float
    retry_max_sleep_sec: float
    log_dir: str
    tray: bool
    value_len: int  # <-- NEW


def setup_logging(log_dir: str):
    """
    Logs:
      - Console (if present)
      - C:\\temp\\scanner\\app.log (daily rotate, keep 7)
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    app_log_path = str(Path(log_dir) / "app.log")

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(threadName)s | %(message)s")

    # Console handler (useful running .py)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # File handler (useful for --noconsole EXE)
    fh = TimedRotatingFileHandler(app_log_path, when="midnight", backupCount=7, encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)


def now_monotonic() -> float:
    return time.monotonic()


def list_serial_ports() -> List[Tuple[str, str]]:
    """
    Returns list of (device, label), e.g. ("COM4", "COM4 - USB Serial Device").
    """
    ports: List[Tuple[str, str]] = []
    for p in list_ports.comports():
        dev = p.device
        desc = p.description or ""
        label = f"{dev} - {desc}" if desc else dev
        ports.append((dev, label))

    def sort_key(x: Tuple[str, str]) -> Tuple[int, int, str]:
        d = x[0].upper()
        if d.startswith("COM"):
            try:
                return (0, int(d[3:]), d)
            except Exception:
                return (1, 0, d)
        return (2, 0, d)

    ports.sort(key=sort_key)
    return ports


class LogDedupe:
    """
    Log-based de-duplication:

    Writes accepted values to:
      C:\\temp\\scanner\\<ISOYEAR>W<ISOWEEK>\\YYYY-MM-DD.log

    Each line (NEW format):
      <ISO_LOCAL_DATETIME_WITH_TZ>\\t<COM_PORT>\\t<VALUE>

    Backward compatible with OLD format:
      <ISO_LOCAL_DATETIME_WITH_TZ>\\t<VALUE>

    De-duplication is by VALUE (global). Log still records COM port for auditing.
    """

    def __init__(self, base_dir: str, window_sec: int, tail_bytes: int = 2_000_000):
        self.base = Path(base_dir)
        self.window_sec = int(window_sec)
        self.tail_bytes = int(tail_bytes)

        self._recent: Dict[str, float] = {}  # value -> last_seen epoch seconds
        self._today_date: Optional[str] = None

        self._fh_today = None
        self._fh_yesterday = None
        self._yesterday_path: Optional[Path] = None

        self.base.mkdir(parents=True, exist_ok=True)
        self._rotate_files_if_needed(force=True)
        self._warm_recent_index_from_logs()

    def close(self):
        try:
            if self._fh_today:
                self._fh_today.close()
        except Exception:
            pass
        try:
            if self._fh_yesterday:
                self._fh_yesterday.close()
        except Exception:
            pass
        self._fh_today = None
        self._fh_yesterday = None
        self._yesterday_path = None

    def _local_now(self) -> datetime:
        return datetime.now().astimezone()

    def _iso_week_folder(self, dt: datetime) -> str:
        iso_year, iso_week, _ = dt.isocalendar()
        return f"{iso_year}W{iso_week:02d}"

    def _day_log_path(self, dt: datetime) -> Path:
        folder = self.base / self._iso_week_folder(dt)
        folder.mkdir(parents=True, exist_ok=True)
        return folder / f"{dt.date().isoformat()}.log"

    def _rotate_files_if_needed(self, force: bool = False):
        now = self._local_now()
        today_str = now.date().isoformat()

        # Today (append)
        if force or self._today_date != today_str:
            if self._fh_today:
                try:
                    self._fh_today.close()
                except Exception:
                    pass
                self._fh_today = None

            today_path = self._day_log_path(now)
            self._fh_today = open(today_path, "a", encoding="utf-8", buffering=1)
            self._today_date = today_str

        # Yesterday (read) only if window crosses midnight
        window_start = now - timedelta(seconds=self.window_sec)
        need_yesterday = window_start.date() != now.date()

        if need_yesterday:
            y_path = self._day_log_path(window_start)
            if self._yesterday_path != y_path:
                if self._fh_yesterday:
                    try:
                        self._fh_yesterday.close()
                    except Exception:
                        pass
                    self._fh_yesterday = None

                self._yesterday_path = y_path
                if y_path.exists():
                    self._fh_yesterday = open(y_path, "r", encoding="utf-8", errors="replace")
        else:
            if self._fh_yesterday:
                try:
                    self._fh_yesterday.close()
                except Exception:
                    pass
            self._fh_yesterday = None
            self._yesterday_path = None

    def _read_tail_lines(self, path: Path) -> List[str]:
        if not path.exists():
            return []
        try:
            with open(path, "rb") as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                if size <= 0:
                    return []
                seek_back = min(size, self.tail_bytes)
                f.seek(-seek_back, os.SEEK_END)
                data = f.read()
        except Exception:
            return []
        text = data.decode("utf-8", errors="replace")
        return text.splitlines()

    def _parse_line(self, line: str) -> Tuple[Optional[float], str]:
        """
        Accepts:
          NEW: ISO<TAB>COMx<TAB>value
          OLD: ISO<TAB>value
        Returns: (timestamp_epoch, value)
        """
        parts = line.split("\t")
        if len(parts) < 2:
            return None, ""

        ts_s = parts[0].strip()
        val = parts[2].strip() if len(parts) >= 3 else parts[1].strip()

        try:
            dt = datetime.fromisoformat(ts_s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=self._local_now().tzinfo)
            return dt.timestamp(), val
        except Exception:
            return None, val

    def _prune_recent(self):
        now_ts = self._local_now().timestamp()
        cutoff = now_ts - self.window_sec
        stale = [k for k, t in self._recent.items() if t < cutoff]
        for k in stale:
            self._recent.pop(k, None)

    def _warm_recent_index_from_logs(self):
        now = self._local_now()
        cutoff_ts = (now - timedelta(seconds=self.window_sec)).timestamp()

        # Today
        today_path = self._day_log_path(now)
        for line in reversed(self._read_tail_lines(today_path)):
            ts, val = self._parse_line(line)
            if ts is None:
                continue
            if ts < cutoff_ts:
                break
            if val:
                self._recent[val] = ts

        # Yesterday if needed
        window_start = now - timedelta(seconds=self.window_sec)
        if window_start.date() != now.date():
            y_path = self._day_log_path(window_start)
            for line in reversed(self._read_tail_lines(y_path)):
                ts, val = self._parse_line(line)
                if ts is None:
                    continue
                if ts < cutoff_ts:
                    break
                if val and val not in self._recent:
                    self._recent[val] = ts

        self._prune_recent()
        logging.info("LogDedupe warmed: %d values in last %ds", len(self._recent), self.window_sec)

    def is_duplicate(self, value: str) -> bool:
        self._rotate_files_if_needed()
        self._prune_recent()

        now_ts = self._local_now().timestamp()
        last = self._recent.get(value)
        return last is not None and (now_ts - last) < self.window_sec

    def record(self, value: str, port: str):
        """
        Record accepted value and include COM port in the log line.
        Dedupe remains by value (global).
        """
        self._rotate_files_if_needed()
        now = self._local_now()
        line = f"{now.isoformat()}\t{port}\t{value}\n"

        try:
            if self._fh_today:
                self._fh_today.write(line)
                self._fh_today.flush()
        except Exception as e:
            logging.warning("Failed writing dedupe log: %s", e)

        self._recent[value] = now.timestamp()

def make_icon(color=(0, 200, 0)) -> Image.Image:
    img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.ellipse((8, 8, 56, 56), fill=color)
    d.ellipse((18, 18, 46, 46), fill=(255, 255, 255, 40))
    return img


def tray_title(cfg: Config) -> str:
    with status_lock:
        s = status["state"]
        last = status["last_value"]
        ok = status["posts_ok"]
        fail = status["posts_fail"]
        err = status["last_error"]
        conn = status["connected"]

    exp = cfg.value_len if cfg.value_len > 0 else "off"
    t = f"Scanner: {s}\nPort: {cfg.port} ({'OK' if conn else 'NO'})\nLen: {exp}\nOK: {ok}  Fail: {fail}"
    if last:
        t += f"\nLast: {last}"
    if err:
        t += f"\nErr: {err[:120]}"
    return t


def tray_color() -> Tuple[int, int, int]:
    with status_lock:
        connected = status.get("connected", False)
        err = status.get("last_error", "")
        st = status.get("state", "")
    if err:
        return (220, 0, 0)
    if not connected or "Reconnecting" in st or "Connecting" in st or "Switching" in st:
        return (255, 180, 0)
    return (0, 200, 0)


def pick_port_tk(current_port: str) -> Optional[str]:
    try:
        import tkinter as tk
        from tkinter import ttk, messagebox
    except Exception:
        return None

    ports = list_serial_ports()
    devs = [d for d, _ in ports]
    labels = [label for _, label in ports]

    root = tk.Tk()
    root.title("Select Serial Port")
    root.geometry("460x260")
    root.resizable(False, False)
    root.attributes("-topmost", True)

    ttk.Label(root, text="Available serial ports:").pack(padx=12, pady=(12, 6), anchor="w")

    lst = tk.Listbox(root, height=9)
    lst.pack(padx=12, pady=6, fill="both", expand=True)

    if not ports:
        lst.insert(tk.END, "(No ports found)")
        lst.configure(state="disabled")
    else:
        for label in labels:
            lst.insert(tk.END, label)

        if current_port in devs:
            idx = devs.index(current_port)
            lst.selection_set(idx)
            lst.see(idx)
        else:
            lst.selection_set(0)

    result = {"port": None}

    def on_refresh():
        nonlocal ports, devs, labels
        ports = list_serial_ports()
        devs = [d for d, _ in ports]
        labels = [label for _, label in ports]

        lst.configure(state="normal")
        lst.delete(0, tk.END)
        if not ports:
            lst.insert(tk.END, "(No ports found)")
            lst.configure(state="disabled")
            return
        for label in labels:
            lst.insert(tk.END, label)
        if current_port in devs:
            idx = devs.index(current_port)
            lst.selection_set(idx)
            lst.see(idx)
        else:
            lst.selection_set(0)

    def on_ok():
        if not ports:
            messagebox.showwarning("No ports", "No serial ports were found.")
            return
        idxs = lst.curselection()
        result["port"] = devs[idxs[0]] if idxs else None
        root.destroy()

    def on_cancel():
        result["port"] = None
        root.destroy()

    btns = ttk.Frame(root)
    btns.pack(padx=12, pady=(0, 12), fill="x")

    ttk.Button(btns, text="Refresh", command=on_refresh).pack(side="left")
    ttk.Button(btns, text="Cancel", command=on_cancel).pack(side="right")
    ttk.Button(btns, text="OK", command=on_ok).pack(side="right", padx=(6, 0))

    root.mainloop()
    return result["port"]


def tray_run(cfg: Config):
    def on_exit(icon, item):
        stop_event.set()
        icon.stop()

    def on_open_logs(icon, item):
        try:
            Path(cfg.log_dir).mkdir(parents=True, exist_ok=True)
            os.startfile(cfg.log_dir)  # type: ignore[attr-defined]
        except Exception:
            pass

    def on_copy_last(icon, item):
        try:
            import tkinter as tk
            with status_lock:
                txt = status.get("last_value", "")
            r = tk.Tk()
            r.withdraw()
            r.clipboard_clear()
            r.clipboard_append(txt)
            r.update()
            r.destroy()
        except Exception:
            pass

    def on_select_port(icon, item):
        new_port = pick_port_tk(cfg.port)
        if new_port and new_port != cfg.port:
            logging.info("Tray requested port change: %s -> %s", cfg.port, new_port)
            cfg.port = new_port
            port_change_event.set()
            with status_lock:
                status["state"] = f"Switching to {new_port}..."
                status["last_error"] = ""

    icon = pystray.Icon(
        "scanner",
        make_icon(tray_color()),
        "Scanner",
        menu=pystray.Menu(
            pystray.MenuItem("Select COM Port...", on_select_port),
            pystray.MenuItem("Open log folder", on_open_logs),
            pystray.MenuItem("Copy last value", on_copy_last),
            pystray.MenuItem("Exit", on_exit),
        ),
    )

    def updater():
        while not stop_event.is_set():
            try:
                icon.icon = make_icon(tray_color())
                icon.title = tray_title(cfg)
            except Exception:
                pass
            time.sleep(1.0)
        try:
            icon.stop()
        except Exception:
            pass

    threading.Thread(target=updater, name="TrayUpdater", daemon=True).start()
    icon.run()


def post_worker(cfg: Config, q: "queue.Queue[Tuple[str, float]]"):
    session = requests.Session()

    headers = {}
    if cfg.header_name and cfg.header_value:
        headers[cfg.header_name] = cfg.header_value

    while not stop_event.is_set():
        try:
            value, _t = q.get(timeout=0.2)
        except queue.Empty:
            continue

        backoff = 1.0
        while not stop_event.is_set():
            try:
                resp = session.post(
                    cfg.endpoint,
                    data={"value": value},
                    headers=headers,
                    timeout=cfg.post_timeout_sec,
                    verify=cfg.verify_tls,
                )

                if 200 <= resp.status_code < 300:
                    logging.info("POST ok (%s) value=%r", resp.status_code, value)
                    with status_lock:
                        status["posts_ok"] += 1
                        status["last_post"] = datetime.now().astimezone().isoformat()
                        status["last_error"] = ""
                    break
                else:
                    logging.warning(
                        "POST failed (%s) body=%s value=%r",
                        resp.status_code,
                        (resp.text[:300] if resp.text else ""),
                        value,
                    )
                    with status_lock:
                        status["posts_fail"] += 1
                        status["last_error"] = f"POST {resp.status_code}"
            except requests.RequestException as e:
                logging.warning("POST exception %s value=%r", e, value)
                with status_lock:
                    status["posts_fail"] += 1
                    status["last_error"] = str(e)

            time.sleep(min(backoff, cfg.retry_max_sleep_sec))
            backoff = min(backoff * 2, cfg.retry_max_sleep_sec)

        q.task_done()


def serial_reader(cfg: Config, q: "queue.Queue[Tuple[str, float]]"):
    dedupe = LogDedupe(base_dir=cfg.log_dir, window_sec=cfg.dedupe_window_sec)

    min_interval = 1.0 / max(cfg.scan_hz_cap, 1.0)
    last_process_t = 0.0

    try:
        while not stop_event.is_set():
            try:
                if port_change_event.is_set():
                    port_change_event.clear()

                logging.info("Opening serial port %s @ %d", cfg.port, cfg.baud)
                with status_lock:
                    status["state"] = f"Connecting {cfg.port}..."
                    status["connected"] = False

                with serial.Serial(cfg.port, cfg.baud, timeout=cfg.read_timeout) as ser:
                    logging.info("Serial port opened (%s).", cfg.port)
                    with status_lock:
                        status["state"] = f"Connected {cfg.port} @ {cfg.baud}"
                        status["connected"] = True
                        status["last_error"] = ""

                    buffer = b""
                    while not stop_event.is_set():
                        if port_change_event.is_set():
                            logging.info("Port change requested, reconnecting to %s...", cfg.port)
                            port_change_event.clear()
                            break

                        chunk = ser.read(256)
                        if chunk:
                            buffer += chunk

                            while b"\n" in buffer:
                                line, buffer = buffer.split(b"\n", 1)
                                line = line.strip(b"\r\t ")

                                if not line:
                                    continue

                                t = now_monotonic()
                                if t - last_process_t < min_interval:
                                    continue
                                last_process_t = t

                                value = line.decode("utf-8", errors="replace").strip()
                                if not value:
                                    continue

                                # NEW: enforce expected value length (default 12)
                                if cfg.value_len > 0 and len(value) != cfg.value_len:
                                    logging.debug(
                                        "Ignored value (len=%d, expected=%d): %r",
                                        len(value), cfg.value_len, value
                                    )
                                    continue

                                with status_lock:
                                    status["last_value"] = value

                                if dedupe.is_duplicate(value):
                                    logging.debug("Duplicate ignored value=%r", value)
                                    continue

                                # Record WITH COM port
                                dedupe.record(value, cfg.port)

                                try:
                                    q.put_nowait((value, t))
                                    logging.info("Enqueued value=%r", value)
                                except queue.Full:
                                    logging.warning("Queue full; dropped value=%r", value)
                        else:
                            time.sleep(0.01)

            except serial.SerialException as e:
                logging.error("Serial error: %s. Reconnecting in 2s...", e)
                with status_lock:
                    status["state"] = "Reconnecting..."
                    status["connected"] = False
                    status["last_error"] = str(e)
                time.sleep(2)

            except Exception as e:
                logging.exception("Unexpected error: %s. Reconnecting in 2s...", e)
                with status_lock:
                    status["state"] = "Reconnecting..."
                    status["connected"] = False
                    status["last_error"] = str(e)
                time.sleep(2)

    finally:
        dedupe.close()
        with status_lock:
            status["connected"] = False
            status["state"] = "Stopped"


def handle_signal(signum, frame):
    logging.info("Received signal %s, shutting down...", signum)
    stop_event.set()


def parse_args() -> Config:
    p = argparse.ArgumentParser(
        description="COM scanner with log-based de-duplication, normal POST, tray port picker + length filter"
    )

    p.add_argument("--port", default="COM4", help="Serial port (default: COM4)")
    p.add_argument("--baud", type=int, default=9600, help="Baud rate (default: 9600)")
    p.add_argument("--read-timeout", type=float, default=0.05, help="Serial read timeout seconds (default: 0.05)")
    p.add_argument("--scan-hz-cap", type=float, default=10.0, help="Max processing rate Hz (default: 10)")
    p.add_argument("--dedupe-sec", type=int, default=60, help="De-duplication window seconds (default: 60)")

    # NEW: expected scan value length
    p.add_argument(
        "--value-len",
        type=int,
        default=12,
        help="Expected scan value length (default: 12). Use 0 to disable length check.",
    )

    p.add_argument(
        "--endpoint",
        default="http://10.72.4.182/pms/prod/21_makor_scan/api.php?action=insertScan",
        help="HTTP endpoint to POST to (default: your API)",
    )

    p.add_argument("--header-name", default=None, help="Optional extra header name (e.g., Authorization)")
    p.add_argument("--header-value", default=None, help="Optional extra header value (e.g., Bearer <token>)")
    p.add_argument("--no-verify-tls", action="store_true", help="Disable TLS verification (NOT recommended)")
    p.add_argument("--post-timeout", type=float, default=5.0, help="HTTP POST timeout seconds (default: 5)")
    p.add_argument("--retry-max-sleep", type=float, default=30.0, help="Max retry backoff seconds (default: 30)")
    p.add_argument("--log-dir", default=r"C:\temp\scanner", help=r"Base log directory (default: C:\temp\scanner)")
    p.add_argument("--no-tray", action="store_true", help="Disable the system tray indicator")

    args = p.parse_args()

    return Config(
        port=args.port,
        baud=args.baud,
        read_timeout=args.read_timeout,
        scan_hz_cap=args.scan_hz_cap,
        dedupe_window_sec=args.dedupe_sec,
        endpoint=args.endpoint,
        header_name=args.header_name,
        header_value=args.header_value,
        verify_tls=(not args.no_verify_tls),
        post_timeout_sec=args.post_timeout,
        retry_max_sleep_sec=args.retry_max_sleep,
        log_dir=args.log_dir,
        tray=(not args.no_tray),
        value_len=args.value_len,
    )


def main():
    cfg = parse_args()
    setup_logging(cfg.log_dir)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    q: "queue.Queue[Tuple[str, float]]" = queue.Queue(maxsize=2000)

    t_post = threading.Thread(target=post_worker, name="Poster", args=(cfg, q), daemon=True)
    t_read = threading.Thread(target=serial_reader, name="SerialReader", args=(cfg, q), daemon=True)

    t_post.start()
    t_read.start()

    logging.info("Running. (tray=%s) port=%s value_len=%s", cfg.tray, cfg.port, cfg.value_len)

    if cfg.tray:
        tray_run(cfg)  # blocks until Exit
    else:
        while not stop_event.is_set():
            time.sleep(0.25)

    stop_event.set()
    logging.info("Stopped.")


if __name__ == "__main__":
    main()
