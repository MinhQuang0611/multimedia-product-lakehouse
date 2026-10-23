import os
import re
import signal
import subprocess
import sys
import time

from src.common.core import LOGS, RAW, ROOT, S, Agent, Context, Task, elapsed, log


def raw_size(source: str) -> tuple[int, int]:
    root = RAW / source
    files = [p for p in root.rglob("*.jsonl.gz")] if root.is_dir() else []
    return len(files), sum(p.stat().st_size for p in files)


class CrawlAgent(Agent):
    """Chạy crawler của một nguồn trong tiến trình con riêng (có timeout): WAF/treo không kéo cả pipeline."""

    name = "crawl"

    def run(self, task: Task, ctx: Context) -> dict:
        source = task.params["source"]
        cmd = [sys.executable, str(ROOT / "run.py"), "crawl", "--only", source]
        if source == "tiki":
            cmd += ["--stages", "listing", "detail", "sellers", "reviews"]  # ảnh do MediaAgent tải cho mọi nguồn
        if task.params.get("limit"):
            cmd += ["--limit", str(task.params["limit"])]
        LOGS.mkdir(parents=True, exist_ok=True)
        logf = LOGS / f"agents_{ctx.run_id}_crawl_{source}.log"
        files0, bytes0 = raw_size(source)
        timeout = S.agents_crawl_timeout_hours.get(source, 6) * 3600
        t0 = time.monotonic()
        self.say(ctx, task, f"chạy {' '.join(cmd[1:])} (timeout {timeout / 3600:.0f}h) → {logf.name}")
        with open(logf, "wb") as out:
            p = subprocess.Popen(cmd, cwd=ROOT, stdout=out, stderr=subprocess.STDOUT, start_new_session=True,
                                 env={**os.environ, "PYTHONUNBUFFERED": "1"})
            try:
                code = p.wait(timeout)
                status = "ok" if code == 0 else "error"
            except subprocess.TimeoutExpired:
                os.killpg(p.pid, signal.SIGTERM)  # SIGTERM để crawler kịp lưu _state.json
                try:
                    p.wait(60)
                except subprocess.TimeoutExpired:
                    os.killpg(p.pid, signal.SIGKILL)
                code, status = p.returncode, "timeout"
        text = logf.read_text(encoding="utf-8", errors="replace")
        files1, bytes1 = raw_size(source)
        res = {"status": status, "exit_code": code, "seconds": elapsed(t0), "log": str(logf),
               "blocked": len(re.findall(r"bị chặn", text)), "locked_by_other": "đang có tiến trình crawl khác" in text,
               "errors": len(re.findall(r" ERROR ", text)), "new_files": files1 - files0, "new_mb": round((bytes1 - bytes0) / 1024**2, 1)}
        if status == "error":
            raise RuntimeError(f"crawler {source} thoát mã {code}, xem {logf}")
        if status == "timeout":
            log.warning("[%s] hết giờ, lần chạy sau tiếp tục từ chỗ dở", task.id)
        return res
