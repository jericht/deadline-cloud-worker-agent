#!/usr/bin/env python3
"""
Reproduces: Spot interruption during job attachment sync not communicated to user.

Two modes:
  --mode sigterm   (default) Send SIGTERM to the worker agent during asset sync.
                   Produces fail_message="Worker Agent received OS signal 15"

  --mode imds      Trigger the worker agent's IMDS spot interruption detection via the
                   DEADLINE_WORKER_IMDS_SPOT_ACTION_URL env var pointing to a local mock.
                   Produces fail_message="The Worker received an EC2 spot interruption"
                   NOTE: You must start the worker agent yourself with:
                     export DEADLINE_WORKER_IMDS_SPOT_ACTION_URL=http://127.0.0.1:51679/spot/instance-action
                     deadline-worker-agent

Prerequisites:
- Run on the worker host (EC2 instance)
- Worker agent must already be running
- deadline CLI installed (pip install deadline)
- For --mode imds: worker agent must have been started with the env var above

Usage:
    # SIGTERM mode
    sudo python3 repro_spot_during_asset_sync.py \\
        --farm-id farm-xxxx --queue-id queue-xxxx --mode sigterm

    # IMDS mock mode (start worker separately with the env var first)
    python3 repro_spot_during_asset_sync.py \\
        --farm-id farm-xxxx --queue-id queue-xxxx --mode imds
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Event, Thread
from typing import Literal


# ---------------------------------------------------------------------------
# Logging tee — all print output goes to both stdout and a log file
# ---------------------------------------------------------------------------

SCRIPT_LOG_FILE = Path(
    f"/tmp/repro_spot_script_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
)


class TeeWriter:
    """Writes to both the original stream and a log file."""

    def __init__(self, stream, log_file: Path) -> None:
        self._stream = stream
        self._log_fh = open(log_file, "a")

    def write(self, msg: str) -> int:
        self._stream.write(msg)
        self._log_fh.write(msg)
        self._log_fh.flush()
        return len(msg)

    def flush(self) -> None:
        self._stream.flush()
        self._log_fh.flush()

    def close(self) -> None:
        self._log_fh.close()


def setup_logging() -> None:
    """Tee stdout and stderr to a log file."""
    sys.stdout = TeeWriter(sys.__stdout__, SCRIPT_LOG_FILE)  # type: ignore[assignment]
    sys.stderr = TeeWriter(sys.__stderr__, SCRIPT_LOG_FILE)  # type: ignore[assignment]
    print(f"[LOG] Script log: {SCRIPT_LOG_FILE}")


# ---------------------------------------------------------------------------
# IMDSMockServer
# ---------------------------------------------------------------------------


class IMDSMockServer:
    """
    A minimal HTTP server that mocks ONLY the EC2 spot/instance-action IMDS endpoint.

    The worker agent is pointed here via the DEADLINE_WORKER_IMDS_SPOT_ACTION_URL env var.
    All other IMDS calls (instance-id, iam/info, etc.) still go to the real IMDS — no
    iptables needed, no risk to other processes on the host.

    Endpoints:
      - GET /spot/instance-action  → 404 normally, 200 with spot JSON once triggered
    """

    PORT = 51679

    def __init__(self) -> None:
        self._trigger_event = Event()
        self._server: HTTPServer | None = None
        self._thread: Thread | None = None

    @property
    def url(self) -> str:
        """The URL the worker agent should use for the spot action endpoint."""
        return f"http://127.0.0.1:{self.PORT}/spot/instance-action"

    def start(self) -> None:
        """Start the mock server."""
        handler = self._make_handler()
        self._server = HTTPServer(("127.0.0.1", self.PORT), handler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        print(f"[IMDS-MOCK] Server started on 127.0.0.1:{self.PORT}")
        print(f"[IMDS-MOCK] Spot action URL: {self.url}")

    def stop(self) -> None:
        """Stop the server."""
        if self._server:
            self._server.shutdown()
            print("[IMDS-MOCK] Server stopped")

    def trigger_spot_interruption(self) -> None:
        """Make the mock start returning spot interruption metadata on the next poll."""
        print("[IMDS-MOCK] Triggering spot interruption response...")
        self._trigger_event.set()
        print("[IMDS-MOCK] Next poll by worker agent will detect spot interruption")

    def _make_handler(self) -> type[BaseHTTPRequestHandler]:
        """Create a request handler class with access to our trigger event."""
        trigger_event = self._trigger_event

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                # Suppress the noisy per-request logs (polls every 1s)
                pass

            def do_GET(self):
                if self.path == "/spot/instance-action":
                    if trigger_event.is_set():
                        termination_time = datetime.now(timezone.utc) + timedelta(seconds=120)
                        body = json.dumps(
                            {
                                "action": "terminate",
                                "time": termination_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            }
                        )
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(body.encode())
                    else:
                        self.send_response(404)
                        self.end_headers()
                else:
                    self.send_response(404)
                    self.end_headers()

        return Handler


# ---------------------------------------------------------------------------
# WorkerAgent
# ---------------------------------------------------------------------------

# Session logs live at: <worker_logs_dir>/<queue_id>/<session_id>.log
# The "Job Attachments Download" banner is written to the SESSION log, not the worker log.
DEFAULT_WORKER_LOGS_DIR = Path("/var/log/amazon/deadline")


class WorkerAgent:
    """
    Interface to the running deadline-worker-agent process on this host.

    Provides PID discovery, session log tailing, and signal delivery.
    """

    def __init__(self, queue_id: str) -> None:
        self._pid: int | None = None
        self._queue_id = queue_id
        self._session_logs_dir = DEFAULT_WORKER_LOGS_DIR / queue_id
        self._worker_log_path = DEFAULT_WORKER_LOGS_DIR / "worker-agent.log"
        self._discover()

    @property
    def pid(self) -> int:
        if self._pid is None:
            raise RuntimeError("Worker agent process not found")
        return self._pid

    def send_sigterm(self) -> None:
        """Send SIGTERM to the worker agent process."""
        pid = self._refresh_pid()
        print(f"[WORKER] Sending SIGTERM to PID {pid}...")
        try:
            os.kill(pid, signal.SIGTERM)
            print("[WORKER] SIGTERM sent successfully")
        except PermissionError:
            print("ERROR: Permission denied. Run with sudo.", file=sys.stderr)
            sys.exit(1)
        except ProcessLookupError:
            print(f"ERROR: PID {pid} no longer exists.", file=sys.stderr)
            sys.exit(1)

    def wait_for_worker_log_marker(self, marker: str, timeout: float = 300) -> bool:
        """
        Tail the worker agent log from the current end, looking for the marker.

        Returns True if the marker was found, False on timeout.
        """
        found_event = Event()
        stop_event = Event()

        def watcher():
            worker_log = self._worker_log_path
            if not worker_log.exists():
                print(f"[WORKER] Worker log not found: {worker_log}", file=sys.stderr)
                return
            print(f"[WORKER] Tailing {worker_log} for: '{marker}'")
            with open(worker_log, "r") as f:
                f.seek(0, 2)
                while not stop_event.is_set() and not found_event.is_set():
                    line = f.readline()
                    if line:
                        if marker in line:
                            print(f"[WORKER] *** MARKER in worker log: {line.strip()}")
                            found_event.set()
                            return
                    else:
                        time.sleep(0.1)

        thread = Thread(target=watcher, daemon=True)
        thread.start()
        found = found_event.wait(timeout=timeout)
        stop_event.set()
        return found

    def wait_for_session_log_marker(self, marker: str, timeout: float = 300) -> bool:
        """
        Two-phase detection:
        1. Watch the worker agent log for a new session starting (extracts session ID)
        2. Tail that session's log file for the asset sync marker

        Returns True if the marker was found, False on timeout.
        """
        import re

        found_event = Event()
        stop_event = Event()
        session_id_holder: list[str] = []

        # Phase 1: Watch worker agent log for "Starting new Session"
        # The log format is: [<session_id>] Starting new Session. [<queue_id>/<job_id>]
        session_id_pattern = re.compile(r"\[([^\]]+)\] Starting new Session")

        def phase1_watcher():
            """Watch worker log for new session, then tail its session log for the marker."""
            worker_log = self._worker_log_path
            if not worker_log.exists():
                print(f"[WORKER] Worker log not found: {worker_log}", file=sys.stderr)
                return

            print(f"[WORKER] Phase 1: Watching {worker_log} for new session...")
            with open(worker_log, "r") as f:
                # Seek to end — we only care about new sessions from now
                f.seek(0, 2)
                while not stop_event.is_set() and not found_event.is_set():
                    line = f.readline()
                    if line:
                        match = session_id_pattern.search(line)
                        if match:
                            session_id = match.group(1)
                            session_id_holder.append(session_id)
                            print(f"[WORKER] Phase 1: New session detected: {session_id}")
                            # Phase 2: tail the session log for the asset sync marker
                            session_log = self._session_logs_dir / f"{session_id}.log"
                            print(f"[WORKER] Phase 2: Watching {session_log} for: '{marker}'")
                            self._tail_file(session_log, marker, found_event, stop_event)
                            return
                    else:
                        time.sleep(0.1)

        thread = Thread(target=phase1_watcher, daemon=True)
        thread.start()
        found = found_event.wait(timeout=timeout)
        stop_event.set()
        return found

    def capture_logs(self, lines: int = 200) -> str:
        """Capture recent worker agent logs and the latest session log."""
        output_file = Path(
            f"/tmp/repro_spot_worker_logs_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
        )

        parts: list[str] = []

        # Worker agent log
        if self._worker_log_path.exists():
            result = subprocess.run(
                ["tail", "-n", str(lines), str(self._worker_log_path)],
                capture_output=True,
                text=True,
            )
            parts.append(f"=== WORKER AGENT LOG ({self._worker_log_path}) ===\n")
            parts.append(result.stdout)

        # Latest session log
        latest_session_log = self._get_latest_session_log()
        if latest_session_log:
            result = subprocess.run(
                ["tail", "-n", str(lines), str(latest_session_log)],
                capture_output=True,
                text=True,
            )
            parts.append(f"\n=== SESSION LOG ({latest_session_log}) ===\n")
            parts.append(result.stdout)

        output_file.write_text("".join(parts))
        print(f"[WORKER] Logs saved to: {output_file}")
        return str(output_file)

    def _discover(self) -> None:
        """Find the running worker agent process."""
        self._pid = self._find_pid()
        if self._pid:
            print(f"[WORKER] Found PID: {self._pid}")
        else:
            print("[WORKER] No running worker agent found")
        print(f"[WORKER] Session logs dir: {self._session_logs_dir}")

    def _refresh_pid(self) -> int:
        """Re-check the PID in case it changed."""
        pid = self._find_pid()
        if pid is None:
            print("ERROR: Worker agent process not found.", file=sys.stderr)
            sys.exit(1)
        self._pid = pid
        return pid

    def _get_latest_session_log(self) -> Path | None:
        """Get the most recently modified session log file."""
        if not self._session_logs_dir.exists():
            return None
        logs = sorted(
            self._session_logs_dir.glob("*.log"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        return logs[0] if logs else None

    @staticmethod
    def _find_pid() -> int | None:
        try:
            result = subprocess.run(
                ["pgrep", "-f", "deadline-worker-agent"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                pids = [int(p) for p in result.stdout.strip().split("\n") if int(p) != os.getpid()]
                return pids[0] if pids else None
        except Exception:
            pass
        return None

    @staticmethod
    def _tail_file(log_path: Path, marker: str, found: Event, stop: Event) -> None:
        """Tail a file from the beginning, looking for the marker, then follow new lines."""
        try:
            with open(log_path, "r") as f:
                while not stop.is_set() and not found.is_set():
                    line = f.readline()
                    if line:
                        if marker in line:
                            print(f"[WORKER] *** MARKER in {log_path.name}: {line.strip()}")
                            found.set()
                            return
                    else:
                        time.sleep(0.1)
        except (OSError, IOError):
            pass


# ---------------------------------------------------------------------------
# ReproRunner (orchestrator)
# ---------------------------------------------------------------------------


class ReproRunner:
    """
    Orchestrates the reproduction:
      1. (Optional) Start IMDS mock
      2. Submit a job with attachments
      3. Watch session logs for asset sync start
      4. Simulate the interruption (SIGTERM or IMDS)
      5. Capture post-interruption logs
    """

    ASSET_SYNC_MARKER = "Job Attachments Download"
    TASK_RUN_MARKER = "Action started. (Kind: TaskRun)"
    ENV_ENTER_MARKER = "Action started. (Kind: EnvEnter)"
    ENV_EXIT_MARKER = "Action started. (Kind: EnvExit)"

    def __init__(
        self,
        *,
        farm_id: str,
        queue_id: str,
        mode: Literal["sigterm", "imds"],
        interrupt_at: Literal["sync", "task", "env_enter", "env_exit"],
        log_capture_seconds: int = 15,
        interrupt_delay: int = 5,
        skip_submit: bool = False,
        with_environment: bool = False,
        env_enter_fails: bool = False,
        attachment_size_gb: int = 20,
    ) -> None:
        self._farm_id = farm_id
        self._queue_id = queue_id
        self._mode = mode
        self._interrupt_at = interrupt_at
        self._log_capture_seconds = log_capture_seconds
        self._interrupt_delay = interrupt_delay
        self._skip_submit = skip_submit
        self._with_environment = with_environment
        self._env_enter_fails = env_enter_fails
        self._attachment_size_gb = attachment_size_gb

        self._worker = WorkerAgent(queue_id=queue_id)
        self._imds_mock: IMDSMockServer | None = None

    def run(self) -> None:
        """Execute the full reproduction sequence."""
        print(f"[REPRO] Mode: {self._mode}")

        if self._worker._pid is None:
            print("ERROR: No running deadline-worker-agent process found.", file=sys.stderr)
            if self._mode == "imds":
                print(
                    "Start the worker agent with:\n"
                    "  export DEADLINE_WORKER_IMDS_SPOT_ACTION_URL="
                    f"http://127.0.0.1:{IMDSMockServer.PORT}/spot/instance-action\n"
                    "  deadline-worker-agent",
                    file=sys.stderr,
                )
            sys.exit(1)

        if self._mode == "imds":
            self._imds_mock = IMDSMockServer()
            self._imds_mock.start()

        try:
            self._submit_job()
            self._wait_for_asset_sync()
            self._simulate_interruption()
            log_output = self._capture_logs()
            self._print_summary(log_output)
        finally:
            if self._imds_mock:
                self._imds_mock.stop()
            self._cleanup_attachments()

    def _submit_job(self) -> None:
        if self._skip_submit:
            print("[REPRO] Skipping job submission (--skip-submit)")
            return

        attachment_dir = Path("/tmp/repro_spot_attachments")
        attachment_dir.mkdir(exist_ok=True)

        # Dummy file to ensure sync takes long enough to interrupt
        payload = attachment_dir / "dummy_payload.bin"
        size_gb = self._attachment_size_gb
        print(f"[REPRO] Creating {size_gb}GB dummy payload...")
        with open(payload, "wb") as f:
            chunk = os.urandom(1 * 1024 * 1024)
            for _ in range(size_gb * 1024):
                f.write(chunk)

        # Job template
        job_template = {
            "specificationVersion": "jobtemplate-2023-09",
            "name": f"repro-spot-during-sync-{datetime.now(timezone.utc).strftime('%H%M%S')}",
            "steps": [
                {
                    "name": "SleepStep",
                    "script": {
                        "actions": {
                            "onRun": {
                                "command": "/bin/sleep",
                                "args": ["60"],
                            }
                        }
                    },
                }
            ],
        }

        # Optionally add an environment that sleeps 60s on enter and exit
        if self._with_environment:
            if self._env_enter_fails:
                on_enter = {
                    "command": "/bin/bash",
                    "args": ["-c", "echo 'Environment enter failing intentionally'; exit 1"],
                }
            else:
                on_enter = {
                    "command": "/bin/sleep",
                    "args": ["60"],
                }

            job_template["jobEnvironments"] = [
                {
                    "name": "SleepEnv",
                    "script": {
                        "actions": {
                            "onEnter": on_enter,
                            "onExit": {
                                "command": "/bin/sleep",
                                "args": ["60"],
                            },
                        }
                    },
                }
            ]
        template_file = attachment_dir / "template.json"
        template_file.write_text(json.dumps(job_template, indent=2))

        # Asset references file — tells the CLI which files are job attachments
        asset_references = {
            "assetReferences": {
                "inputs": {
                    "filenames": [str(payload)],
                    "directories": [],
                },
                "outputs": {
                    "directories": [],
                },
            }
        }
        asset_ref_file = attachment_dir / "asset_references.json"
        asset_ref_file.write_text(json.dumps(asset_references, indent=2))

        print(f"[REPRO] Submitting job (farm={self._farm_id}, queue={self._queue_id})")
        print(f"[REPRO] Attachment: {payload} ({size_gb}GB)")

        cmd = [
            "deadline",
            "bundle",
            "submit",
            str(attachment_dir),
            "--farm-id",
            self._farm_id,
            "--queue-id",
            self._queue_id,
            "--yes",
        ]

        print(f"[REPRO] Running: {' '.join(cmd)}")
        result = subprocess.run(cmd)

        if result.returncode != 0:
            print("[REPRO] ERROR: 'deadline bundle submit' failed", file=sys.stderr)
            sys.exit(1)

        print("[REPRO] Job submitted successfully")

    def _wait_for_asset_sync(self) -> None:
        if self._interrupt_at == "sync":
            marker = self.ASSET_SYNC_MARKER
            label = "asset sync"
            print(f"[REPRO] Waiting for {label} to begin in session logs (timeout: 5 min)...")
            found = self._worker.wait_for_session_log_marker(marker, timeout=300)
        elif self._interrupt_at == "task":
            marker = self.TASK_RUN_MARKER
            label = "task run"
            print(f"[REPRO] Waiting for {label} to begin in worker log (timeout: 5 min)...")
            found = self._worker.wait_for_worker_log_marker(marker, timeout=300)
        elif self._interrupt_at == "env_enter":
            marker = self.ENV_ENTER_MARKER
            label = "environment enter"
            print(f"[REPRO] Waiting for {label} to begin in worker log (timeout: 5 min)...")
            found = self._worker.wait_for_worker_log_marker(marker, timeout=300)
        elif self._interrupt_at == "env_exit":
            marker = self.ENV_EXIT_MARKER
            label = "environment exit"
            print(f"[REPRO] Waiting for {label} to begin in worker log (timeout: 5 min)...")
            found = self._worker.wait_for_worker_log_marker(marker, timeout=300)
        else:
            raise ValueError(f"Unknown interrupt_at: {self._interrupt_at}")

        if not found:
            print(
                f"ERROR: Timed out waiting for {label} to start.",
                file=sys.stderr,
            )
            print("Make sure the job was picked up by this worker.", file=sys.stderr)
            sys.exit(1)

        # Let the action get underway before interrupting
        print(
            f"[REPRO] {label.capitalize()} detected! Waiting {self._interrupt_delay}s before interrupting..."
        )
        time.sleep(self._interrupt_delay)

    def _simulate_interruption(self) -> None:
        if self._mode == "sigterm":
            self._worker.send_sigterm()
        else:
            assert self._imds_mock is not None
            self._imds_mock.trigger_spot_interruption()

    def _capture_logs(self) -> str:
        print(f"[REPRO] Waiting {self._log_capture_seconds}s for shutdown to complete...")
        time.sleep(self._log_capture_seconds)
        return self._worker.capture_logs()

    def _cleanup_attachments(self) -> None:
        attachment_dir = Path("/tmp/repro_spot_attachments")
        if attachment_dir.exists():
            print(f"[REPRO] Cleaning up {attachment_dir}...")
            shutil.rmtree(attachment_dir)
            print("[REPRO] Attachment dir removed")

    def _print_summary(self, log_output: str) -> None:
        print("\n" + "=" * 60)
        print("REPRODUCTION COMPLETE")
        print("=" * 60)
        print(f"Mode: {self._mode}")
        print(f"Logs: {log_output}")
        print(f"Script log: {SCRIPT_LOG_FILE}")
        print()
        if self._mode == "sigterm":
            print('Expected fail_message: "Worker Agent received OS signal 15"')
        else:
            print('Expected fail_message: "The Worker received an EC2 spot interruption"')
        print()
        print("Next steps:")
        print("  1. Check the Deadline Cloud console → task retries modal")
        print("  2. Verify whether the interruption message is shown to the user")
        print()
        print("Bug present:")
        print("  → Retries modal shows 'Never Attempted' with no error message")
        print("Bug fixed:")
        print("  → Retries modal shows the interruption/spot reason")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Reproduce spot interruption during asset sync")
    parser.add_argument("--farm-id", required=True, help="Deadline Cloud farm ID")
    parser.add_argument("--queue-id", required=True, help="Deadline Cloud queue ID")
    parser.add_argument(
        "--mode",
        choices=["sigterm", "imds"],
        default="sigterm",
        help="Interruption method (default: sigterm)",
    )
    parser.add_argument(
        "--interrupt-at",
        choices=["sync", "task", "env_enter", "env_exit"],
        default="sync",
        help="When to interrupt: 'sync' = during asset sync, 'task' = during task run, 'env_enter' = during environment enter, 'env_exit' = during environment exit (default: sync)",
    )
    parser.add_argument(
        "--log-capture-seconds",
        type=int,
        default=15,
        help="Seconds to wait after interruption before capturing logs (default: 15)",
    )
    parser.add_argument(
        "--interrupt-delay",
        type=int,
        default=5,
        help="Seconds to wait after asset sync starts before sending interruption (default: 5)",
    )
    parser.add_argument(
        "--skip-submit",
        action="store_true",
        help="Skip job submission (use if a job is already pending)",
    )
    parser.add_argument(
        "--with-environment",
        action="store_true",
        help="Add a job environment that sleeps 60s on enter and exit (required for --interrupt-at env_enter/env_exit)",
    )
    parser.add_argument(
        "--env-enter-fails",
        action="store_true",
        help="Make the environment onEnter exit with code 1 instead of sleeping (use with --with-environment)",
    )
    parser.add_argument(
        "--attachment-size-gb",
        type=int,
        default=20,
        help="Size of the dummy attachment file in GB (default: 20)",
    )
    args = parser.parse_args()

    setup_logging()

    runner = ReproRunner(
        farm_id=args.farm_id,
        queue_id=args.queue_id,
        mode=args.mode,
        interrupt_at=args.interrupt_at,
        log_capture_seconds=args.log_capture_seconds,
        interrupt_delay=args.interrupt_delay,
        skip_submit=args.skip_submit,
        with_environment=args.with_environment,
        env_enter_fails=args.env_enter_fails,
        attachment_size_gb=args.attachment_size_gb,
    )
    runner.run()


if __name__ == "__main__":
    main()
