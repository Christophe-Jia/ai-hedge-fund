import asyncio
import signal
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, Optional


class ScriptStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    DONE = "done"
    ERROR = "error"


@dataclass
class ProcessEntry:
    script_name: str
    status: ScriptStatus = ScriptStatus.IDLE
    process: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    exit_code: Optional[int] = None


class ProcessManager:
    def __init__(self):
        self._registry: Dict[str, ProcessEntry] = {}

    def get(self, name: str) -> ProcessEntry:
        if name not in self._registry:
            self._registry[name] = ProcessEntry(script_name=name)
        return self._registry[name]

    def is_running(self, name: str) -> bool:
        entry = self._registry.get(name)
        return entry is not None and entry.status == ScriptStatus.RUNNING

    def register_start(self, name: str, process: asyncio.subprocess.Process) -> None:
        entry = self.get(name)
        entry.process = process
        entry.status = ScriptStatus.RUNNING
        entry.started_at = datetime.utcnow()
        entry.finished_at = None
        entry.exit_code = None

    def register_finish(self, name: str, exit_code: int) -> None:
        entry = self.get(name)
        entry.process = None
        entry.status = ScriptStatus.DONE if exit_code == 0 else ScriptStatus.ERROR
        entry.finished_at = datetime.utcnow()
        entry.exit_code = exit_code

    async def kill(self, name: str) -> bool:
        entry = self._registry.get(name)
        if entry is None or entry.status != ScriptStatus.RUNNING or entry.process is None:
            return False
        process = entry.process
        try:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        except ProcessLookupError:
            pass
        self.register_finish(name, process.returncode if process.returncode is not None else -1)
        return True

    def all(self) -> Dict[str, ProcessEntry]:
        return dict(self._registry)


# Module-level singleton
process_manager = ProcessManager()
