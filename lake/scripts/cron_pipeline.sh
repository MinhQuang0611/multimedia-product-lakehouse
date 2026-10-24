#!/usr/bin/env bash
# Chạy pipeline multi-agent từ cron: không phụ thuộc phiên SSH. Khoá nằm trong pipeline (data/_agents/locks),
# nên lần cron chồng lên lần đang chạy sẽ tự bỏ qua.
# Dùng: cron_pipeline.sh [tham số của `python run.py pipeline`]
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PIPELINE_PYTHON:-$(command -v python3)}"
cd "$ROOT" || exit 1
mkdir -p data/_logs
find data/_logs -name 'cron_pipeline_*.log' -mtime +14 -delete 2>/dev/null
LOG="data/_logs/cron_pipeline_$(date +%Y%m%d).log"
echo "=== $(date '+%F %T') pipeline $* ===" >> "$LOG"
PYTHONUNBUFFERED=1 nice -n 10 "$PYTHON" run.py pipeline "$@" >> "$LOG" 2>&1
code=$?
echo "=== $(date '+%F %T') xong, mã thoát $code ===" >> "$LOG"
exit $code
