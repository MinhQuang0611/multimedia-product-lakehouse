from __future__ import annotations

import zipfile
from pathlib import Path

from src.common.config import get_logger, load_config, paths

log = get_logger("ingest.download")


def _unzip_all(folder: Path) -> None:
    for z in folder.glob("*.zip"):
        log.info("  giải nén %s", z.name)
        with zipfile.ZipFile(z) as zf:
            zf.extractall(folder)
        z.unlink()


def download_source(name: str, spec: dict) -> None:
    from kaggle.api.kaggle_api_extended import KaggleApi  # import muộn: cần credentials

    api = KaggleApi()
    api.authenticate()

    out = paths().raw / name
    out.mkdir(parents=True, exist_ok=True)
    missing = [f for f in spec["files"] if not (out / f).exists()]
    if not missing:
        log.info("[%s] đã đủ file, bỏ qua", name)
        return

    if spec["kind"] == "competition":
        for f in missing:
            log.info("[%s] tải %s ...", name, f)
            api.competition_download_file(spec["slug"], f, path=str(out), quiet=False)
    else:
        log.info("[%s] tải dataset %s ...", name, spec["slug"])
        api.dataset_download_files(spec["slug"], path=str(out), quiet=False)
    _unzip_all(out)

    still_missing = [f for f in spec["files"] if not (out / f).exists()]
    if still_missing:
        raise FileNotFoundError(f"[{name}] thiếu sau khi tải: {still_missing}")


def run(only: list[str] | None = None) -> None:
    from requests.exceptions import HTTPError

    failed = []
    for name, spec in load_config()["sources"].items():
        if only and name not in only:
            continue
        try:
            download_source(name, spec)
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 403 and spec["kind"] == "competition":
                log.error("[%s] 403 — chưa chấp nhận luật cuộc thi. Mở "
                          "https://www.kaggle.com/competitions/%s/rules và bấm 'I Understand and Accept'",
                          name, spec["slug"])
            else:
                log.error("[%s] lỗi tải: %s", name, e)
            failed.append(name)
    if failed:
        raise SystemExit(f"Tải thất bại: {failed}")
