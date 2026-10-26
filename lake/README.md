# Tiki Image Lake

Dự án gồm crawler sản phẩm, kho ảnh MinIO, data warehouse PostgreSQL và khung orchestration Airflow.

## Khởi động data warehouse và kho ảnh

Chạy từ thư mục root repository (`F:/RIPT/BTL`), nơi đặt `docker-compose.yml` và `.env`. `lake/` chỉ chứa mã nguồn/dữ liệu pipeline; cấu hình Docker dùng chung và scripts init warehouse được quản lý tại root.

PostgreSQL là data warehouse; mỗi dashboard/chủ đề có schema riêng. Scripts init ở `warehouse/init/` chạy tự động theo thứ tự tên khi Postgres khởi tạo volume lần đầu: tạo role `wh_app` chỉ đọc, các schema (`product_dashboard`, `sales_dashboard`, `customer_dashboard`, `marketing_dashboard`, `shared`) và bảng quản trị ban đầu. Hãy đổi mật khẩu mẫu trong `.env` trước khi chạy.

```powershell
Copy-Item .env.example .env
# Sửa WH_ADMIN_PASSWORD và WH_APP_PASSWORD trong .env
docker compose up -d warehouse
```

Kết nối từ máy host: `localhost:5432`, database `warehouse`. Dùng `wh_owner` cho quản trị; dashboard kết nối bằng `wh_app` (read-only). SQL init chỉ chạy tự động khi volume `warehouse-data` được tạo lần đầu. Thay đổi init sau đó cần migration riêng; muốn khởi tạo lại từ đầu thì phải xoá volume (sẽ mất dữ liệu).

Các schema hiện là khung ban đầu; tạo bảng mart cụ thể theo chủ đề trong `warehouse/init/` sau khi thống nhất metric/dashboard. Đây là nguồn duy nhất cho init warehouse; không đặt scripts warehouse trong `lake/`.

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
└── viewer/        server.py + static/index.html: web xem bộ mẫu data/sample (lưới sản phẩm, tổng quan, SQL)
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
python scripts/make_sample.py                # tạo bộ mẫu data/sample từ gold (100 sản phẩm/nguồn, ≤4 ảnh)
python run.py viewer                      # http://127.0.0.1:8770 (--port, --host), đọc data/sample
setsid nohup python run.py viewer >data/_logs/viewer.log 2>&1 &     # chạy nền, sống sót khi ngắt SSH
```

Tổng quan theo nguồn, lọc/xem sản phẩm kèm ảnh, giá, tồn kho, lịch sử giá, review; duyệt từng bảng gold; ô SQL chỉ đọc
(thêm bảng `pl`: một dòng mỗi sản phẩm với giá mới nhất). Tab "Tìm bằng ảnh": tải/kéo thả/dán ảnh hoặc gõ mô tả, chọn vùng
món đồ phát hiện được (thân trên/thân dưới/chân), lọc nguồn/nhóm/giá/còn hàng; nút "Tìm sản phẩm giống" trong trang chi tiết.
Viewer chuyển yêu cầu sang coordinator (`SEARCH_COORDINATOR`, mặc định `http://127.0.0.1:18700`, xem "Tìm kiếm phân tán"),
nên tab này cần cụm node và coordinator đang chạy; các tab khác không cần. Trang tự nạp lại khi pipeline ghi gold mới. Mặc định chỉ nghe
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

## Tìm kiếm ảnh (`src/search/`)

Chạy trên bộ mẫu `data/sample`, env riêng có torch + CUDA (`requirements-search.txt`):

```bash
uv venv --python 3.11 .venv-search
VIRTUAL_ENV=.venv-search uv pip install --index-strategy unsafe-best-match -r requirements-search.txt   # hoặc pip install -r
.venv-search/bin/python run.py search embed                      # embedding mọi ảnh, mọi model (chỉ tính sha256 còn thiếu)
.venv-search/bin/python run.py search crops                      # phát hiện món đồ + embedding vùng cắt + thí nghiệm
.venv-search/bin/python run.py search eval                       # so sánh model
.venv-search/bin/python run.py search query --image anh.jpg      # tìm bằng ảnh: ảnh gốc + mọi vùng phát hiện được
.venv-search/bin/python run.py search query --image anh.jpg --region lower --source tiki zara --price-max 500000
.venv-search/bin/python run.py search query --text "áo khoác jean nam" --category-l1 outerwear
.venv-search/bin/python run.py search query --selfcheck         # chạy lại đánh giá khác góc qua chỉ mục FAISS
```

Lưu trữ theo lakehouse: embedding và kết quả phát hiện là bảng đặc trưng tầng gold, khoá theo sha256 của file ảnh (khớp
`fact_media.sha256` của kho media theo nội dung), nên bộ mẫu và bộ đầy đủ dùng chung, ảnh đã tính không tính lại. Chỉ mục FAISS
không được lưu: nó dựng lại từ các file này khi khởi động (dưới 1 giây với bộ mẫu). `gold.duckdb` có view cho các bảng này
(`GoldAgent` giữ lại khi dựng lại catalog), vector là `FLOAT[dim]` nên truy vấn được bằng `array_cosine_similarity`.

| Đường dẫn (`data/lakehouse/gold/`) | View trong `gold.duckdb` | Nội dung |
|---|---|---|
| `embeddings/model=<tên>/data.parquet` | `feat_image_<tên>` | sha256 → vector ảnh gốc, `model_version` |
| `embeddings_crops/model=<tên>/data.parquet` | `feat_crop_<tên>` | `<sha256>#<upper\|lower\|feet>` → vector vùng cắt |
| `detections/grounding_dino_tiny/data.parquet` | `fact_garment_detection` | khung món đồ (chuẩn hoá [0, 1]), nhãn, điểm |

Báo cáo đánh giá ghi vào `data/reports/search/<thời điểm>/`.

| File | Việc |
|---|---|
| `catalog.py` | đường dẫn kho; đọc ảnh và sản phẩm bộ mẫu; view cho `gold.duckdb` |
| `encoders.py` | `dinov2_b14`, `siglip2_b16`, `fashion_siglip` (Marqo), `clip_b32`: ảnh/văn bản → vector L2-normalize |
| `embed.py` | kho embedding theo nội dung (chỉ tính phần thiếu) |
| `crops.py` | phát hiện món đồ (Grounding DINO tiny), gom vùng thân trên/thân dưới/chân, embedding vùng cắt, so 5 biến thể |
| `garments.py` | nhóm món đồ, gom khung theo vùng, cắt ảnh (không cần torch) |
| `shard.py` | chỉ mục FAISS `IndexFlatIP` (chính xác) của một phần dữ liệu, gộp điểm theo sản phẩm, lọc nguồn/danh mục/đối tượng/giá/còn hàng (không cần torch) |
| `engine.py` | phía truy vấn (`QueryEncoder`: phát hiện món đồ + embedding) và tìm kiếm tập trung (`SearchEngine`) |
| `evaluate.py` | đánh giá: khác góc, biến dạng, văn bản → ảnh, độ trễ; KTC 95% bootstrap theo sản phẩm |

Điểm sản phẩm = cặp (vector truy vấn, vector của sản phẩm) giống nhất. Mỗi vector truy vấn lấy `k × vmax` vector đầu (vmax =
số vector nhiều nhất của một sản phẩm) là đủ để top-k sản phẩm chính xác tuyệt đối; bộ lọc chạy trong FAISS qua `IDSelector`.

Không có nhãn "cùng sản phẩm" từ ngoài, nên bộ đánh giá lấy từ chính dữ liệu: 400 sản phẩm (brands, tiki, uniqlo, zara) có
2–4 ảnh khác góc; mỗi ảnh làm truy vấn (bỏ chính nó khỏi kho), đúng khi tìm ra sản phẩm của nó. H&M và Shopee (1 ảnh/sản phẩm)
chỉ nằm trong kho làm nhiễu. Kết quả trên bộ mẫu (R@1 sản phẩm, khác góc): `fashion_siglip` 78,4% > `siglip2_b16` 71,4% >
`clip_b32` 51,4% > `dinov2_b14` 47,8%; văn bản → ảnh (R@10): `siglip2_b16` 81% (tiếng Việt tốt nhất) > `fashion_siglip` 66%.
Vì vậy dùng `fashion_siglip` cho truy vấn ảnh và `siglip2_b16` cho truy vấn văn bản. Nhãn có nhiễu (màu khác nhau của cùng
sản phẩm Uniqlo, tin trùng giữa seller Tiki, ảnh rác) nên con số là cận dưới.

Ảnh người mặc thường có nhiều món đồ (một nửa số truy vấn). Thêm vùng cắt cả ở kho (vùng khớp danh mục sản phẩm) lẫn ở
truy vấn (mọi vùng phát hiện được, không dùng danh mục), điểm = cặp vector giống nhất: `fashion_siglip` R@1 78,4% → 82,7%
(+4,4 điểm, KTC 95% [2,5; 6,4]), `siglip2_b16` 71,4% → 76,8%.

### Tìm kiếm phân tán

Phân mảnh theo nguồn (mỗi nguồn là một cửa hàng/hệ thống bán; một sản phẩm thuộc đúng một nguồn). Node chỉ giữ chỉ mục FAISS
của phần mình, không cần torch/GPU: coordinator (trên host, có GPU) phát hiện món đồ và tính vector một lần, gửi song song tới
mọi node (vector float32 dạng base64), gộp top-k của từng node thành top-k toàn cục. Mỗi node trả top-k chính xác của nó và
mỗi sản phẩm ở đúng một node, nên kết quả gộp y hệt tìm trên một chỉ mục chung.

```bash
docker compose -f ../docker-compose.search.yml --profile n3 up -d --build --wait   # n1 | n3 | n6 (lake/src/search/topology.py)
.venv-search/bin/python run.py search coordinator --topology n3                   # http://127.0.0.1:18700
curl -F file=@anh.jpg -F k=10 -F 'filters={"source":["tiki"]}' 127.0.0.1:18700/search/image
curl -H 'content-type: application/json' -d '{"text":"áo khoác jean nam"}' 127.0.0.1:18700/search/text
docker compose -f ../docker-compose.search.yml stop node-b                         # kịch bản node lỗi
.venv-search/bin/python run.py search bench     # E1 chính xác, E2 độ trễ 1/3/6 node, E3 node chết/chậm → data/reports/search/
```

| Thành phần | File | Ghi chú |
|---|---|---|
| Node | `node.py`, `shard.py` | `/health`, `/search`; từ chối vector khác `model_version` (409); `NODE_DELAY_MS` giả lập node chậm |
| Coordinator | `coordinator.py` | `/search/image`, `/search/text`, `/search/vectors`, `/health`, `/stats`; kiểm tra đầu vào (k ≤ 100, kiểu bộ lọc, ảnh ≤ 15 MB và ≤ 40 MP); timeout không thử lại, lỗi kết nối thử lại 1 lần; cầu dao theo node (3 lỗi liên tiếp → mở; sau 10 s và health check qua → nửa mở cho 1 truy vấn thử; lỗi 4xx do truy vấn không tính); cache LRU (không lưu kết quả thiếu node) |
| Hạ tầng | `../docker-compose.search.yml`, `deploy/search/Dockerfile.node` | image node ~550 MB, code và dữ liệu gắn read-only; cổng `127.0.0.1:187xx` |

Kết quả trả về có `partial` (thiếu node nào) và trạng thái từng node (`ok`, `error`, `timeout`, `bad_response`, `rejected`,
`circuit_open`, `version_mismatch`). `model_version` (tên model + commit trọng số) đi kèm mọi vector: node từ chối vector khác
phiên bản chỉ mục của mình, và `embed`/`crops` không ghi thêm vào kho tính bằng phiên bản khác (xoá thư mục `model=<tên>` để tính lại).

Hạn chế đã biết: truy vấn văn bản tiếng Việt ngắn do người gõ ("giày sneaker trắng") kém hơn hẳn tiếng Anh ("white sneakers");
đánh giá văn bản ở trên dùng tên sản phẩm (dài, sát tiêu đề) nên không thấy điều này.

## Airflow

`airflow/config.py` chứa cấu hình dùng chung cho orchestration và quy ước watermark. Các DAG sẽ được thêm sau vào `airflow/dags/`; script crawl, ingest, transform và watermark được tách trong `airflow/scripts/`.

Watermark bền vững sẽ nằm tại `s3://lake/_control/watermarks/...`; mỗi job phải claim input trước khi chạy và chỉ commit watermark sau khi ghi output hoàn tất.
