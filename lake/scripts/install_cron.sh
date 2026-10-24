#!/usr/bin/env bash
# Cài (hoặc cập nhật) lịch cron cho pipeline. Chạy lại an toàn: chỉ thay khối giữa hai dòng đánh dấu.
#   scripts/install_cron.sh            cài đặt
#   scripts/install_cron.sh --remove   gỡ
#   scripts/install_cron.sh --show     in khối sẽ cài
set -eu
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="$(command -v python3)"
BEGIN="# >>> multimedia-product-lakehouse >>>"
END="# <<< multimedia-product-lakehouse <<<"
RUN="$ROOT/scripts/cron_pipeline.sh"

block() {
cat <<BLOCK
$BEGIN
SHELL=/bin/bash
PIPELINE_PYTHON=$PYTHON
# Crawl mọi nguồn mỗi 4 giờ: chồng lên lần đang chạy thì bỏ qua, bị WAF chặn/hết giờ thì lần sau tiếp tục từ chỗ dở.
0 */4 * * * $RUN --stages crawl
# Mở máy lại: tiếp tục crawl sau 2 phút.
@reboot sleep 120 && $RUN --stages crawl
# Xử lý: tải media → bronze → chuẩn hoá → silver → gold → báo cáo, mỗi 3 giờ (lệch giờ với crawl).
30 */3 * * * $RUN --stages media bronze normalize silver gold report
$END
BLOCK
}

current="$(crontab -l 2>/dev/null || true)"
stripped="$(printf '%s\n' "$current" | sed "/$BEGIN/,/$END/d")"
case "${1:-}" in
  --show) block ;;
  --remove) printf '%s\n' "$stripped" | sed '/./,$!d' | crontab - && echo "Đã gỡ lịch cron." ;;
  *) { printf '%s\n' "$stripped" | sed '/./,$!d'; block; } | crontab - && echo "Đã cài lịch cron:" && crontab -l | sed -n "/$BEGIN/,/$END/p" ;;
esac
