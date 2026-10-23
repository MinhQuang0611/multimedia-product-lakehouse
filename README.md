# Tiki Image Lake

Dự án gồm crawler sản phẩm Tiki, kho ảnh MinIO và khung orchestration Airflow. Không còn Data Warehouse, PostgreSQL, Superset, OLAP hay data mining.

## Khởi động kho ảnh

```powershell
Copy-Item .env.example .env
docker compose up -d minio
```

MinIO Console: http://localhost:9001 (`lakeadmin` / `lakeadmin123`, hoặc giá trị trong `.env`). Bucket là `lake`.

## Cài môi trường và crawl

```powershell
conda env create -f environment.yml
conda activate tiki-images
python run.py crawl --limit 10
python run.py crawl
```

Crawler implementation nằm tại `airflow/scripts/crawl/tiki.py`. Ảnh được ghi trực tiếp vào:

```
s3://lake/images/tiki/<YYYY-MM-DD>/<product-id>/<index>.<extension>
```

Các phản hồi JSON và state crawl được giữ cục bộ trong `data/raw/tiki/` để crawler có thể tiếp tục và làm mới dữ liệu. Có thể dùng `LAKE_BACKEND=local` để ghi ảnh vào `data/lake/images/` thay cho MinIO.

Danh mục crawl (`crawl_tiki_roots`, `crawl_tiki_exclude` trong `airflow/config.py`) chỉ gồm đồ nam; đồ nữ và đồ lót/đồ bơi đã được loại.

`listing`, `detail`, `reviews` chia file theo cây danh mục Tiki, ví dụ
`data/raw/tiki/2026-10-19/detail/915-thoi-trang-nam/917-ao-thun-nam.jsonl.gz`. File `<gốc>.jsonl.gz` cạnh thư mục
`<gốc>/` là trang 1 của danh mục cha (crawler đọc để biết có phải chia nhỏ không). Sản phẩm của detail/reviews nằm
ở danh mục sâu nhất mà nó xuất hiện trong listing.

| File (`data/raw/tiki/<ngày>/`) | Nội dung |
|---|---|
| `listing/<danh mục>/….jsonl.gz` | mỗi dòng một trang danh sách 40 sản phẩm: giá, giá gốc, rating, đã bán, seller |
| `detail/<danh mục>/….jsonl.gz` | chi tiết sản phẩm mới hoặc quá 7 ngày: ảnh, biến thể size/màu, tồn kho, mô tả |
| `reviews/<danh mục>/….jsonl.gz` | review của 2000 sản phẩm nhiều review nhất |
| `sellers.jsonl.gz`, `categories.jsonl.gz` | shop; cây danh mục |

Dữ liệu crawl trước khi chia theo danh mục (một file `listing.jsonl.gz`, `detail.jsonl.gz`, `reviews.jsonl.gz` mỗi
ngày) chuyển sang định dạng mới bằng:

```bash
python airflow/scripts/crawl/tiki_split.py --dry-run     # chia thử vào thư mục tạm, in số dòng/số file, không đổi gì
python airflow/scripts/crawl/tiki_split.py               # file cũ chuyển sang data/raw/tiki/_old_format/<ngày>/
python airflow/scripts/crawl/tiki_split.py --delete-old  # hoặc xoá luôn file cũ
```

Script giữ nguyên từng dòng, kiểm tra số dòng trước và sau, dừng nếu đang có crawl chạy (khoá `.lock`), chạy lại
an toàn (ngày đã chuyển bỏ qua, bị ngắt giữa bước ghi thì ghi nốt). `--root` để chỉ thư mục khác.

Giới hạn đo được: listing tối đa 2000 sản phẩm/truy vấn (crawler tự chia xuống danh mục con) và bị WAF chặn
sau ~25 request bất kể nhịp, nên chạy theo đợt 20 request rồi nghỉ 15 phút, lấy detail trong lúc nghỉ. Khi bị
chặn Tiki vẫn trả HTTP 200 với trang HTML. Review không lưu tên người mua, id người mua được băm.

### Chạy nền không cần Airflow

`--forever` chạy mỗi ngày một snapshot; bị WAF chặn thì nghỉ `crawl_tiki_blocked_cooldown` giây rồi tiếp tục từ chỗ dở.

```bash
python airflow/scripts/crawl/tiki.py --forever
cp deploy/tiki-crawl.service ~/.config/systemd/user/   # systemd user service, sửa đường dẫn nếu khác máy
systemctl --user daemon-reload && systemctl --user enable --now tiki-crawl
```

Trên Windows, chạy `python run.py crawl` hằng ngày bằng Task Scheduler (02:00, máy tắt thì chạy bù khi bật lại):

```powershell
powershell -ExecutionPolicy Bypass -File scripts\register_tiki_task.ps1 -RunNow
Get-ScheduledTaskInfo "BTL Tiki crawl"       # trạng thái, lần chạy tới
Stop-ScheduledTask "BTL Tiki crawl"          # dừng lần đang chạy
Unregister-ScheduledTask "BTL Tiki crawl"    # gỡ lịch
```

Log ở `data/_logs/tiki_crawl_<thời điểm>.log`. Chỉ một tiến trình crawl mỗi nguồn được chạy tại một thời điểm (khoá
`data/raw/<nguồn>/.lock`).

## Crawl web hãng thời trang, Zara, Uniqlo

```bash
python run.py crawl                          # mọi nguồn song song: brands, zara, uniqlo, tiki
python run.py crawl --only brands zara       # từng nguồn
python airflow/scripts/crawl/brands.py --sites hades yame
python airflow/scripts/crawl/zara.py --limit 3     # chạy thử
python airflow/scripts/crawl/uniqlo.py --limit 3
```

| Nguồn | File (`data/raw/<nguồn>/<ngày>/`) | Nội dung |
|---|---|---|
| `brands` | `<site>.jsonl.gz`, mỗi dòng một trang `products.json` | 30 web hãng (Haravan/Sapo/Shopify, danh sách `crawl_brands_sites` trong `airflow/config.py`): sản phẩm, biến thể size/màu, giá, giá gốc, ảnh; Haravan có thêm tồn kho thật và số đã bán |
| `zara` | `categories.jsonl.gz`, `products/<woman\|man\|kid>/<id>-<slug>.jsonl.gz` | Zara Việt Nam (VND): cây danh mục, lưới sản phẩm theo danh mục, còn/hết hàng |
| `uniqlo` | `listing/`, `stock/`, `details/`, `reviews/` — mỗi thư mục một file theo nhóm women/men/kids/baby | Uniqlo Mỹ (USD): tồn kho theo SKU (chặn trần 11), chi tiết 7 ngày/lần, review khi số review đổi (không lưu nickname) |

Chạy lại trong ngày thì tiếp tục từ chỗ dở. Zara và Uniqlo là API nội bộ của web (không phải API công khai) nên giữ
nhịp 1–2 s/request; không gọi API tồn kho của Zara vì `robots.txt` cấm.

## Ảnh H&M và Shopee (Kaggle)

Dùng làm dữ liệu đối chiếu cho tìm kiếm ảnh, đã lọc bỏ đồ nữ và đồ nhạy cảm như Tiki (luật lọc trong `SOURCES`).
Tải zip vào `data/raw/_zips/` (cần token Kaggle, xem `KAGGLE_API_TOKEN`) và CSV vào `data/raw/hm/`, `data/raw/shopee/`, rồi:

```bash
python airflow/scripts/ingest/kaggle_images.py          # 256px → data/raw/{hm,shopee}/images/
python airflow/scripts/ingest/kaggle_images.py --full   # ảnh gốc → images_full/
```

## Pipeline multi-agent (crawl → bronze → chuẩn hoá → silver → gold)

```bash
python run.py pipeline                                   # toàn bộ, mọi nguồn
python run.py pipeline --stages bronze normalize silver gold report
python run.py pipeline --stages crawl --sources zara --limit 3
python run.py pipeline --plan                            # chỉ in đồ thị task
```

`src/orchestrator/supervisor.py` dựng đồ thị task rồi giao cho các agent chạy song song theo phụ thuộc; mọi sự kiện ghi vào
`data/_agents/runs/<run>/events.jsonl` (bus), kết quả từng task ở `summary.json`, báo cáo ở `data/reports/pipeline/latest.md`.

```
src/
├── common/        core.py (đường dẫn, Task/Bus/Agent/khoá), schema.py (schema chuẩn 5 entity)
├── orchestrator/  supervisor.py (đồ thị task, chạy song song), cli.py (python run.py pipeline)
├── crawl/         agent.py: chạy crawler từng nguồn trong tiến trình con
├── bronze/        agent.py: raw jsonl.gz/CSV → parquet
├── normalize/     agent.py + adapters/ (tiki, brands, zara, uniqlo, legacy cho H&M/Shopee/Olist)
├── media/         agent.py: tải ảnh/video, kho theo nội dung
├── silver/        agent.py: làm sạch, quarantine
├── gold/          agent.py: dim/fact/agg + gold.duckdb
├── report/        agent.py: báo cáo chạy
└── viewer/        server.py + static/index.html: web xem dữ liệu gold
```

| Agent | Việc | Ghi chú |
|---|---|---|
| `crawl` | chạy crawler từng nguồn (tiến trình con, có timeout) | lỗi/WAF không chặn các task sau (dùng dữ liệu đã có); lần sau tiếp tục từ chỗ dở |
| `bronze` | raw `jsonl.gz`/CSV → parquet y nguyên + cột lineage | `data/lakehouse/bronze/<nguồn>/<dataset>/snapshot_date=…`; bỏ qua ngày không đổi |
| `normalize` | bronze → **schema chuẩn chung**, mỗi nguồn một adapter (`src/normalize/adapters/`) | 11 entity, xem bên dưới (`src/common/schema.py`) |
| `media` | tải ảnh/video theo URL, đăng ký ảnh cục bộ H&M/Shopee | kho theo nội dung `media/files/<nguồn>/<sha[:2]>/<sha256>.<ext>` + manifest; trần file/dung lượng/đĩa trống trong `airflow/config.py` |
| `silver` | làm sạch, khử trùng lặp, kiểm tra chất lượng, gắn đường dẫn media | dòng lỗi vào `silver/_quarantine/`; cảnh báo mềm ở cột `dq_flags`; log ở `data/reports/quality_log.csv` |
| `gold` | lược đồ sao + bảng tổng hợp, giá quy đổi VND (`fx_to_vnd`) | `data/lakehouse/gold/<bảng>/data.parquet`; mở `gold/gold.duckdb` để truy vấn |
| `report` | tổng hợp trạng thái task + độ phủ từng nguồn | luôn chạy, kể cả khi task trước lỗi |

Nguồn: `tiki`, `brands` (38 web hãng), `zara`, `uniqlo` (crawl) và `hm`, `shopee`, `olist` (CSV Kaggle). **Mọi bảng của mọi nguồn**
đều vào một schema chung gồm 11 entity (`src/common/schema.py`; đổi cột thì tăng `SCHEMA_VERSION` để dựng lại):

| Entity | Nội dung | Nguồn có dữ liệu |
|---|---|---|
| `product` | sản phẩm × ngày snapshot (giá, tồn kho, rating, danh mục chuẩn, brand sạch, đối tượng) | cả 7 nguồn |
| `variant` | màu/size/SKU, giá và tồn kho theo biến thể | tiki, brands, zara, uniqlo, hm |
| `media` | ảnh/video (URL hoặc file cục bộ, kết quả tải) | tiki, brands, zara, uniqlo, hm, shopee |
| `review` | đánh giá theo sản phẩm (Tiki, Uniqlo) hoặc theo đơn (Olist) | tiki, uniqlo, olist |
| `seller` | cửa hàng/người bán | tiki, brands, zara, uniqlo, olist |
| `category` | cây danh mục gốc của từng nguồn | tiki, brands, zara, uniqlo, hm, olist |
| `customer` | khách hàng (tuổi, vùng, trạng thái thành viên) | hm, olist |
| `order`, `order_item`, `payment` | đơn hàng, dòng hàng, thanh toán | hm (đơn = khách × ngày × kênh), olist |
| `location` | toạ độ theo mã bưu chính | olist |

Cột chuẩn hoá chung trong `product` (bộ luật ở `src/normalize/standardize.py`, áp ở bước normalize):
`category_l1`/`category_l2` (danh mục chuẩn 2 cấp: tops, bottoms, outerwear, footwear, dresses_skirts, accessories, bags,
underwear_swim, sleepwear, activewear, sets, non_fashion, other…), `brand_std` (bỏ khẩu hiệu, tên miền, "Official"),
`audience` (men/women/kids/unisex/unknown). Silver thêm `audience_std`: nguồn không ghi đối tượng thì lấy đối tượng chiếm
≥70% của cùng cửa hàng (cần ≥30 sản phẩm đã biết), dòng được suy ra có `audience_inferred = true`. Danh mục chuẩn là luật
từ khoá (vi/en/pt/id) nên Shopee (tiêu đề Indonesia, nhiều hàng không phải thời trang) phần lớn còn `other`.

Giới hạn dữ liệu nguồn: giá H&M trong giao dịch là số đã ẩn danh (không có đơn vị tiền nên không quy đổi VND); tồn kho
Uniqlo bị API chặn trần ở 11; thời gian Olist ghi theo giờ địa phương Brazil nhưng được lưu như UTC.

Gold: `dim_source`, `dim_date`, `dim_brand`, `dim_category` (theo nguồn), `dim_category_std` (danh mục chuẩn), `dim_category_tree`,
`dim_product`, `dim_seller`, `dim_customer`, `dim_location`, `fact_price_snapshot`, `fact_variant_stock`, `fact_media`, `fact_review`,
`fact_order`, `fact_order_item`, `fact_payment`, `agg_category_price`, `agg_brand_summary`, `agg_sales_month_category`.
Tỷ giá quy đổi VND (`fx_to_vnd`: VND, USD, BRL) là hằng số trong `airflow/config.py`, chỉ để so sánh tương đối.

### Xem dữ liệu gold trên web

```bash
python run.py viewer                      # http://127.0.0.1:8770 (--port, --host)
setsid nohup python run.py viewer >data/_logs/viewer.log 2>&1 &     # chạy nền, sống sót khi ngắt SSH
```

Tổng quan theo nguồn, lọc/xem sản phẩm kèm ảnh, giá, tồn kho, lịch sử giá, review; duyệt từng bảng gold; ô SQL chỉ đọc
(thêm bảng `pl`: một dòng mỗi sản phẩm với giá mới nhất). Trang tự nạp lại khi pipeline ghi gold mới. Mặc định chỉ nghe
`127.0.0.1`; từ máy khác dùng `ssh -L 8770:localhost:8770 <máy>` (VS Code Remote tự chuyển cổng).

### Chạy nền bằng cron (sống sót khi ngắt SSH)

```bash
scripts/install_cron.sh            # cài khối cron (chạy lại an toàn); --show để xem, --remove để gỡ
```

| Lịch | Lệnh |
|---|---|
| mỗi 4 giờ (`0 */4 * * *`) và khi máy khởi động lại | `--stages crawl` |
| mỗi 3 giờ (`30 */3 * * *`) | `--stages media bronze normalize silver gold report` |

Lịch chồng lên lần đang chạy thì tự bỏ qua (khoá `data/_agents/locks/`; crawl và xử lý khoá riêng nên chạy song song được).
Log: `data/_logs/cron_pipeline_<ngày>.log` (giữ 14 ngày) và `data/_logs/agents_<run>_crawl_<nguồn>.log`. Chạy thủ công
không cần giữ phiên SSH: `setsid nohup scripts/cron_pipeline.sh --stages crawl >/dev/null 2>&1 &`.

## Airflow

`airflow/config.py` chứa cấu hình dùng chung cho orchestration và quy ước watermark. Các DAG sẽ được thêm sau vào `airflow/dags/`; script crawl, ingest, transform và watermark được tách trong `airflow/scripts/`.

Watermark bền vững sẽ nằm tại `s3://lake/_control/watermarks/...`; mỗi job phải claim input trước khi chạy và chỉ commit watermark sau khi ghi output hoàn tất.
