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

### Chạy nền không cần Airflow

`--forever` chạy mỗi ngày một snapshot; bị WAF chặn thì nghỉ `crawl_tiki_blocked_cooldown` giây rồi tiếp tục từ chỗ dở.

```bash
PYTHONPATH=. python airflow/scripts/crawl/tiki.py --forever
cp deploy/tiki-crawl.service ~/.config/systemd/user/   # systemd user service, sửa đường dẫn nếu khác máy
systemctl --user daemon-reload && systemctl --user enable --now tiki-crawl
```

## Ảnh H&M và Shopee (Kaggle)

Dùng làm dữ liệu đối chiếu cho tìm kiếm ảnh, đã lọc bỏ đồ nữ và đồ nhạy cảm như Tiki (luật lọc trong `SOURCES`).
Tải zip vào `data/raw/_zips/` (cần token Kaggle, xem `KAGGLE_API_TOKEN`) và CSV vào `data/raw/hm/`, `data/raw/shopee/`, rồi:

```bash
PYTHONPATH=. python airflow/scripts/ingest/kaggle_images.py          # 256px → data/raw/{hm,shopee}/images/
PYTHONPATH=. python airflow/scripts/ingest/kaggle_images.py --full   # ảnh gốc → images_full/
```

## Airflow

`airflow/config.py` chứa cấu hình dùng chung cho orchestration và quy ước watermark. Các DAG sẽ được thêm sau vào `airflow/dags/`; script crawl, ingest, transform và watermark được tách trong `airflow/scripts/`.

Watermark bền vững sẽ nằm tại `s3://lake/_control/watermarks/...`; mỗi job phải claim input trước khi chạy và chỉ commit watermark sau khi ghi output hoàn tất.
