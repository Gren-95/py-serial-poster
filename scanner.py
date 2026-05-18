from __future__ import annotations

import argparse
import logging
import queue
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import requests
import serial


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


stop_event = threading.Event()


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
    )


def now_monotonic() -> float:
    return time.monotonic()


class LogDedupe:
    """
    Log-based de-duplication:
    - Writes accepted values to daily log files in:
        C:\\temp\\scanner\\<ISOYEAR>W<ISOWEEK>\\YYYY-MM-DD.log
    - Builds an in-memory recent index from tail of today's (and sometimes yesterday's) log
    - Keeps at most 2 files open: today (append), yesterday (read) only when needed
    """

    def __init__(self, base_dir: str, window_sec: int, tail_bytes: int = 2_000_000):
        self.base = Path(base_dir)
        self.window_sec = int(window_sec)
        self.tail_bytes = int(tail_bytes)

        self._recent: Dict[str, float] = {}  # value -> last_seen epoch seconds (local tz)
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

        # Open today's file in append mode
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

        # Open yesterday read-only only if the window crosses midnight
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
                f.seek(0, 2)
                size = f.tell()
                seek_back = min(size, self.tail_bytes)
                f.seek(-seek_back, 2)
                data = f.read()
        except Exception:
            return []

        text = data.decode("utf-8", errors="replace")
        return text.splitlines()

    def _parse_line(self, line: str) -> Tuple[Optional[float], str]:
        # Format: ISO8601<TAB>value
        if "\t" not in line:
            return None, ""
        ts_s, val = line.split("\t", 1)
        val = val.strip()

        try:
            dt = datetime.fromisoformat(ts_s.strip())
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

    def record(self, value: str):
        self._rotate_files_if_needed()
        now = self._local_now()
        line = f"{now.isoformat()}\t{value}\n"

        try:
            if self._fh_today:
                self._fh_today.write(line)
                self._fh_today.flush()
        except Exception as e:
            logging.warning("Failed writing dedupe log: %s", e)

        self._recent[value] = now.timestamp()


def post_worker(cfg: Config, q: "queue.Queue[Tuple[str, float]]"):
    """
    Sends application/x-www-form-urlencoded POST so PHP can read $_POST['value'].
    """
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
                    break
                else:
                    logging.warning(
                        "POST failed (%s) body=%s value=%r",
                        resp.status_code,
                        (resp.text[:300] if resp.text else ""),
                        value,
                    )
            except requests.RequestException as e:
                logging.warning("POST exception %s value=%r", e, value)

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
                logging.info("Opening serial port %s @ %d", cfg.port, cfg.baud)
                with serial.Serial(cfg.port, cfg.baud, timeout=cfg.read_timeout) as ser:
                    logging.info("Serial port opened.")
                    buffer = b""

                    while not stop_event.is_set():
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

                                if dedupe.is_duplicate(value):
                                    logging.debug("Duplicate ignored value=%r", value)
                                    continue

                                # record immediately to avoid floods
                                dedupe.record(value)

                                try:
                                    q.put_nowait((value, t))
                                    logging.info("Enqueued value=%r", value)
                                except queue.Full:
                                    logging.warning("Queue full; dropped value=%r", value)
                        else:
                            time.sleep(0.01)

            except serial.SerialException as e:
                logging.error("Serial error: %s. Reconnecting in 2s...", e)
                time.sleep(2)
            except Exception as e:
                logging.exception("Unexpected error: %s. Reconnecting in 2s...", e)
                time.sleep(2)
    finally:
        dedupe.close()


def handle_signal(signum, frame):
    logging.info("Received signal %s, shutting down...", signum)
    stop_event.set()


def parse_args() -> Config:
    p = argparse.ArgumentParser(description="COM scanner with log-based de-duplication and endpoint POST")

    # Defaults as requested
    p.add_argument("--port", default="COM4", help="Serial port (default: COM4)")
    p.add_argument("--baud", type=int, default=9600, help="Baud rate (default: 9600)")
    p.add_argument("--read-timeout", type=float, default=0.05, help="Serial read timeout seconds (default: 0.05)")
    p.add_argument("--scan-hz-cap", type=float, default=10.0, help="Max processing rate (Hz) (default: 10)")
    p.add_argument("--dedupe-sec", type=int, default=60, help="De-duplication window seconds (default: 60)")

    p.add_argument(
        "--endpoint",
        default="http://10.72.4.182/pms/prod/21_makor_scan/api.php?action=insertScan",
        help="HTTP endpoint to POST to",
    )

    p.add_argument("--header-name", default=None, help="Optional extra header name (e.g., Authorization)")
    p.add_argument("--header-value", default=None, help="Optional extra header value (e.g., Bearer <token>)")
    p.add_argument("--no-verify-tls", action="store_true", help="Disable TLS verification (NOT recommended)")
    p.add_argument("--post-timeout", type=float, default=5.0, help="HTTP POST timeout seconds (default: 5)")
    p.add_argument("--retry-max-sleep", type=float, default=30.0, help="Max retry backoff seconds (default: 30)")
    p.add_argument("--log-dir", default=r"C:\temp\scanner", help=r"Base log directory (default: C:\temp\scanner)")

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
    )


def main():
    setup_logging()
    cfg = parse_args()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    q: "queue.Queue[Tuple[str, float]]" = queue.Queue(maxsize=2000)

    t_post = threading.Thread(target=post_worker, name="Poster", args=(cfg, q), daemon=True)
    t_read = threading.Thread(target=serial_reader, name="SerialReader", args=(cfg, q), daemon=True)

    t_post.start()
    t_read.start()

    logging.info("Running. Press Ctrl+C to stop.")
    while not stop_event.is_set():
        time.sleep(0.2)

    logging.info("Stopped.")


if __name__ == "__main__":
    main()
