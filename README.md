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

## Airflow

`airflow/config.py` chứa cấu hình dùng chung cho orchestration và quy ước watermark. Các DAG sẽ được thêm sau vào `airflow/dags/`; script crawl, ingest, transform và watermark được tách trong `airflow/scripts/`.

Watermark bền vững sẽ nằm tại `s3://lake/_control/watermarks/...`; mỗi job phải claim input trước khi chạy và chỉ commit watermark sau khi ghi output hoàn tất.
