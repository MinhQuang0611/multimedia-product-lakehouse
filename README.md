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

## Airflow

`airflow/config.py` chứa cấu hình dùng chung cho orchestration và quy ước watermark. Các DAG sẽ được thêm sau vào `airflow/dags/`; script crawl, ingest, transform và watermark được tách trong `airflow/scripts/`.

Watermark bền vững sẽ nằm tại `s3://lake/_control/watermarks/...`; mỗi job phải claim input trước khi chạy và chỉ commit watermark sau khi ghi output hoàn tất.
