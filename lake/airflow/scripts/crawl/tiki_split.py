import gzip
import json
import os
import re
import shutil
import sys
from pathlib import Path

AIRFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from lib.config import get_logger, load_config, paths
from tiki import SPLIT_KINDS, _lock, listing_products, node_name, read_lines

log = get_logger("crawl.tiki_split")

DAY = re.compile(r"\d{4}-\d{2}-\d{2}")
TMP = ".split_tmp"
MARK = "COMMITTING"
UNKNOWN = Path("_khong_ro_danh_muc.jsonl.gz")


def load_tree(files: list[Path]) -> dict[int, tuple[int, str | None]]:
    tree = {}
    for f in files:
        for x in read_lines(f):
            for c in x["payload"].get("data") or []:
                tree[c["id"]] = (x["_req"]["parent_id"], c.get("url_key"))
    return tree


def node_of(cat: int, tree: dict, roots: dict) -> Path:
    # Đi ngược parent_id tới danh mục gốc: cùng đường dẫn mà crawler mới tạo khi duyệt cây.
    parts, seen = [], set()
    while cat in tree and cat not in roots and cat not in seen:
        seen.add(cat)
        parent, slug = tree[cat]
        parts.append(node_name(cat, slug))
        cat = parent
    parts.append(node_name(cat, roots.get(cat)))
    return Path(*reversed(parts))


def listing_file(node: Path) -> Path:
    return node.parent / f"{node.name}.jsonl.gz"


def raw_lines(path: Path):
    # Giữ nguyên byte từng dòng; dừng ở dòng cuối bị cụt như read_lines.
    try:
        with gzip.open(path, "rb") as f:
            for raw in f:
                try:
                    yield raw if raw.endswith(b"\n") else raw + b"\n", json.loads(raw)
                except json.JSONDecodeError:
                    return
    except (EOFError, gzip.BadGzipFile):
        return


def split_kind(old: Path, tmp: Path, route) -> tuple[int, dict[Path, int]]:
    members = old.read_bytes().count(b"\x1f\x8b\x08\x00")
    writers, counts, n = {}, {}, 0
    try:
        for raw, x in raw_lines(old):
            rel = route(x) or UNKNOWN
            if rel not in writers:
                (tmp / rel).parent.mkdir(parents=True, exist_ok=True)
                writers[rel] = gzip.open(tmp / rel, "wb")
                counts[rel] = 0
            writers[rel].write(raw)
            counts[rel] += 1
            n += 1
    finally:
        for w in writers.values():
            w.close()
    if n < members - 1:
        # Mỗi dòng là một gzip member; đọc được ít hơn nhiều nghĩa là có member hỏng ở giữa file.
        raise RuntimeError(f"{old}: có {members} gzip member nhưng chỉ đọc được {n} dòng — file hỏng ở giữa, "
                           f"không chuyển ngày này")
    return n, counts


def commit(day: Path, root: Path, delete_old: bool) -> None:
    tmp = day / TMP
    for kind in SPLIT_KINDS:
        src = tmp / kind
        if src.exists():
            for f in sorted(src.rglob("*.jsonl.gz")):
                dst = day / kind / f.relative_to(src)
                dst.parent.mkdir(parents=True, exist_ok=True)
                if dst.exists():
                    # Ngày đã có file định dạng mới (crawler mới chạy trước): nối gzip member vào cuối.
                    with open(dst, "ab") as out, open(f, "rb") as inp:
                        shutil.copyfileobj(inp, out)
                    f.unlink()
                else:
                    os.replace(f, dst)
        old = day / f"{kind}.jsonl.gz"
        if old.exists():
            if delete_old:
                old.unlink()
            else:
                bak = root / "_old_format" / day.name / old.name
                bak.parent.mkdir(parents=True, exist_ok=True)
                os.replace(old, bak)
    shutil.rmtree(tmp)


def split_day(day: Path, root: Path, tree: dict, roots: dict, dry_run: bool, delete_old: bool) -> None:
    tmp = day / TMP
    if (tmp / MARK).exists():
        log.info("[%s] lần trước dừng giữa bước ghi, ghi nốt", day.name)
        commit(day, root, delete_old)
        return
    olds = {k: day / f"{k}.jsonl.gz" for k in SPLIT_KINDS if (day / f"{k}.jsonl.gz").exists()}
    if not olds:
        return
    if tmp.exists():
        shutil.rmtree(tmp)
    tree = {**tree, **load_tree([day / "categories.jsonl.gz"])}
    pmap = None
    for kind in SPLIT_KINDS:
        if kind == "detail" or kind == "reviews":
            if pmap is None:
                # Gán sản phẩm vào danh mục bằng đúng hàm crawler dùng, trên listing đã chia (cũ + mới nếu có).
                pmap = {pid: p["_file"] for out in (day, tmp) for pid, p in listing_products(out).items()}
        if kind not in olds:
            continue
        if kind == "listing":
            route = lambda x: listing_file(node_of(x["_req"]["category"], tree, roots))
        elif kind == "detail":
            route = lambda x: pmap.get(x["_req"]["id"])
        else:
            route = lambda x: pmap.get(x["_req"]["product_id"])
        n, counts = split_kind(olds[kind], tmp / kind, route)
        written = sum(1 for rel in counts for _ in raw_lines(tmp / kind / rel))
        if written != n:
            raise RuntimeError(f"{olds[kind]}: đọc {n} dòng nhưng đọc lại file mới chỉ được {written} dòng")
        before = olds[kind].stat().st_size
        after = sum((tmp / kind / rel).stat().st_size for rel in counts)
        unknown = counts.get(UNKNOWN, 0)
        log.info("[%s] %-8s %7d dòng → %3d file, %6.1f MB → %6.1f MB%s", day.name, kind, n, len(counts),
                 before / 1e6, after / 1e6, f", {unknown} dòng không rõ danh mục" if unknown else "")
    if dry_run:
        shutil.rmtree(tmp)
        return
    (tmp / MARK).write_text("", encoding="utf-8")
    commit(day, root, delete_old)


def run(root: Path | None = None, dry_run: bool = False, delete_old: bool = False) -> None:
    root = root or paths().raw / "tiki"
    roots = dict(load_config()["crawl"]["tiki"]["roots"])
    lock = _lock(root / ".lock")
    if lock is None:
        sys.exit("đang có tiến trình crawl chạy (khoá data/raw/tiki/.lock), dừng crawl rồi chạy lại")
    try:
        days = sorted(d for d in root.iterdir() if d.is_dir() and DAY.fullmatch(d.name))
        tree = load_tree([d / "categories.jsonl.gz" for d in days])
        log.info("%s: %d ngày, cây %d danh mục%s", root, len(days), len(tree), " (chạy thử, không ghi)" if dry_run else "")
        failed = []
        for day in days:
            try:
                split_day(day, root, tree, roots, dry_run, delete_old)
            except RuntimeError as e:
                log.error("[%s] %s", day.name, e)
                shutil.rmtree(day / TMP, ignore_errors=True)
                failed.append(day.name)
    finally:
        lock.close()
    if failed:
        sys.exit(f"{len(failed)} ngày không chuyển được, giữ nguyên định dạng cũ: {', '.join(failed)}")
    if not dry_run and not delete_old and (root / "_old_format").exists():
        log.info("File cũ chuyển sang %s — kiểm tra xong thì xoá được", root / "_old_format")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Chia listing/detail/reviews.jsonl.gz cũ thành file theo danh mục")
    ap.add_argument("--root", type=Path, help="thư mục data/raw/tiki (mặc định theo config)")
    ap.add_argument("--dry-run", action="store_true", help="chỉ chia thử vào thư mục tạm rồi xoá, không đổi gì")
    ap.add_argument("--delete-old", action="store_true", help="xoá file cũ thay vì chuyển sang _old_format/")
    a = ap.parse_args()
    run(a.root, a.dry_run, a.delete_old)
