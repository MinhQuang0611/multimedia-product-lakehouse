import time
import traceback
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime

from src.common.core import CRAWL_SOURCES, LOCK_GROUPS, S, Agent, Bus, Context, GroupLock, Task, log, now_iso, write_json


def build_plan(sources: list[str], stages: list[str], opts: dict) -> list[Task]:
    """Đồ thị task: crawl → bronze → normalize → media → silver → gold → report. Deps ngoài stages được chọn bị bỏ."""
    t: list[Task] = []
    for s in sources:
        if s in CRAWL_SOURCES:
            t.append(Task(f"crawl:{s}", "crawl", {"source": s, "limit": opts.get("limit")}, soft=True))
        t.append(Task(f"bronze:{s}", "bronze", {"source": s}, [f"crawl:{s}"] if s in CRAWL_SOURCES else []))
        t.append(Task(f"normalize:{s}", "normalize", {"source": s}, [f"bronze:{s}"]))
        t.append(Task(f"media:{s}", "media", {"source": s}, [f"normalize:{s}"], soft=True))
    t.append(Task("silver", "silver", {}, [f"normalize:{s}" for s in sources] + [f"media:{s}" for s in sources]))
    t.append(Task("gold", "gold", {}, ["silver"]))
    t.append(Task("report", "report", {}, ["gold"], always=True))
    keep = {x.id for x in t if x.agent in stages}
    for x in t:
        x.deps = [d for d in x.deps if d in keep]
        x.retries = 0 if x.agent == "crawl" else S.agents_task_retries
    return [x for x in t if x.id in keep]


class Supervisor(Agent):
    name = "supervisor"

    def __init__(self, agents: dict[str, Agent]):
        self.agents = agents

    def execute(self, task: Task, ctx: Context) -> None:
        agent = self.agents[task.agent]
        task.started = now_iso()
        ctx.bus.publish("task.started", task.id)
        while True:
            task.attempts += 1
            try:
                task.result = agent.run(task, ctx) or {}
                task.status = "ok"
                break
            except Exception as e:
                task.error = f"{e.__class__.__name__}: {e}"
                log.error("[%s] lỗi lần %d: %s\n%s", task.id, task.attempts, task.error, traceback.format_exc())
                ctx.bus.publish("task.error", task.id, attempt=task.attempts, error=task.error)
                if task.attempts > task.retries:
                    task.status = "failed"
                    break
                time.sleep(15 * task.attempts)
        task.ended = now_iso()
        ctx.bus.publish(f"task.{task.status}", task.id, result=task.result, error=task.error)

    def run_plan(self, tasks: list[Task], sources: list[str], opts: dict) -> Context:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = S.agents_dir / "runs" / run_id
        ctx = Context(run_id, run_dir, Bus(run_dir), sources, {t.id: t for t in tasks}, opts)
        groups = [g for g, st in LOCK_GROUPS.items() if any(t.agent in st for t in tasks)]
        locks = [GroupLock(g) for g in groups]
        if not all(lk.acquire() for lk in locks):
            for lk in locks:
                lk.release()
            log.warning("đang có lần chạy khác giữ khoá %s, bỏ qua", groups)
            ctx.opts["skipped_locked"] = True
            return ctx
        try:
            self._loop(ctx)
        finally:
            for lk in locks:
                lk.release()
            write_json(run_dir / "summary.json", {
                "run_id": run_id, "sources": sources, "opts": opts,
                "tasks": {t.id: {"status": t.status, "attempts": t.attempts, "started": t.started, "ended": t.ended,
                                 "error": t.error, "result": t.result} for t in tasks}})
        return ctx

    def _loop(self, ctx: Context) -> None:
        tasks = ctx.tasks
        ctx.bus.publish("run.started", "supervisor", tasks=list(tasks))
        running: dict[Future, Task] = {}
        with ThreadPoolExecutor(S.agents_workers, thread_name_prefix="agent") as ex:
            changed = True
            while True:
                for t in tasks.values():
                    if t.status != "pending":
                        continue
                    deps = [tasks[d] for d in t.deps]
                    if any(d.status in ("pending", "running") for d in deps):
                        continue
                    blocked = [d.id for d in deps if d.status in ("failed", "skipped") and not d.soft]
                    if blocked and not t.always:
                        t.status = "skipped"
                        t.error = f"phụ thuộc lỗi: {', '.join(blocked)}"
                        ctx.bus.publish("task.skipped", t.id, reason=t.error)
                        changed = True
                        continue
                    t.status = "running"
                    running[ex.submit(self.execute, t, ctx)] = t
                if not running:
                    # Task vừa bị skipped có thể là deps của task đã xét trước đó trong lượt này: xét lại tới khi hết đổi.
                    if any(t.status == "pending" for t in tasks.values()) and changed:
                        changed = False
                        continue
                    break
                done, _ = wait(running, return_when=FIRST_COMPLETED)
                for f in done:
                    f.result()
                    running.pop(f)
        ctx.bus.publish("run.finished", "supervisor", **{k: sum(t.status == k for t in tasks.values())
                                                         for k in ("ok", "failed", "skipped")})
