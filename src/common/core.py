import fcntl
import json
import shutil
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from lib.config import ROOT, airflow_settings, get_logger

S = airflow_settings()
LAKE = S.lakehouse_dir
LOGS = ROOT / "data" / "_logs"
REPORTS = ROOT / "data" / "reports"
RAW = S.raw_dir

CRAWL_SOURCES = ["tiki", "brands", "zara", "uniqlo"]
LEGACY_SOURCES = ["hm", "shopee", "olist"]
SOURCES = CRAWL_SOURCES + LEGACY_SOURCES
STAGES = ["crawl", "bronze", "normalize", "media", "silver", "gold", "report"]
# Stage chạy trong cùng một nhóm khoá: crawl (hàng giờ, bị WAF chặn) tách khỏi xử lý để không chặn nhau.
LOCK_GROUPS = {"crawl": {"crawl"}, "process": {"bronze", "normalize", "media", "silver", "gold", "report"}}

log = get_logger("agents")


def layer(name: str, *parts: str) -> Path:
    return LAKE.joinpath(name, *parts)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def swap_dir(tmp: Path, final: Path) -> None:
    # Ghi vào thư mục tạm rồi thay cả thư mục: người đọc không bao giờ thấy trạng thái ghi dở.
    if final.exists():
        shutil.rmtree(final)
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp.rename(final)


def read_json(path: Path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    tmp.replace(path)


@dataclass
class Task:
    id: str
    agent: str
    params: dict = field(default_factory=dict)
    deps: list[str] = field(default_factory=list)
    soft: bool = False  # lỗi không chặn task phụ thuộc (crawl/media: dữ liệu cũ vẫn dùng được)
    always: bool = False  # chạy dù deps lỗi (báo cáo)
    retries: int = 0
    status: str = "pending"  # pending|running|ok|failed|skipped
    result: dict = field(default_factory=dict)
    error: str | None = None
    attempts: int = 0
    started: str | None = None
    ended: str | None = None


class Bus:
    """Kênh thông điệp giữa các agent: mọi sự kiện ghi bền vào events.jsonl; agent khác đọc được qua history()."""

    def __init__(self, run_dir: Path):
        run_dir.mkdir(parents=True, exist_ok=True)
        self.path = run_dir / "events.jsonl"
        self.lock = threading.Lock()

    def publish(self, topic: str, sender: str, **data) -> None:
        line = json.dumps({"ts": now_iso(), "topic": topic, "from": sender, **data}, ensure_ascii=False, default=str)
        with self.lock, open(self.path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    def history(self, topic: str | None = None) -> list[dict]:
        if not self.path.exists():
            return []
        with self.lock:
            rows = [json.loads(x) for x in self.path.read_text(encoding="utf-8").splitlines() if x.strip()]
        return [r for r in rows if topic is None or r["topic"] == topic]


@dataclass
class Context:
    run_id: str
    run_dir: Path
    bus: Bus
    sources: list[str]
    tasks: dict[str, Task] = field(default_factory=dict)
    opts: dict = field(default_factory=dict)

    def result_of(self, task_id: str) -> dict:
        t = self.tasks.get(task_id)
        return t.result if t else {}


class Agent:
    name = "agent"

    def run(self, task: Task, ctx: Context) -> dict:
        raise NotImplementedError

    def say(self, ctx: Context, task: Task, msg: str, **data) -> None:
        log.info("[%s] %s", task.id, msg)
        ctx.bus.publish("progress", task.id, msg=msg, **data)


class GroupLock:
    """flock theo nhóm stage: cron chồng lên lần chạy đang dở thì bỏ qua thay vì chạy song song."""

    def __init__(self, group: str):
        self.path = S.agents_dir / "locks" / f"{group}.lock"
        self.f = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.f = open(self.path, "a+")
        try:
            fcntl.flock(self.f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.f.close()
            self.f = None
            return False
        return True

    def release(self) -> None:
        if self.f:
            self.f.close()
            self.f = None


def elapsed(t0: float) -> float:
    return round(time.monotonic() - t0, 1)
