# BTL — Data Lakehouse & khai phá dữ liệu sản phẩm đa phương tiện

## Cài đặt

```bash
conda env create -f environment.yml
conda activate btl
docker compose up -d minio          # Data Lake trên MinIO: API :9000, giao diện web http://localhost:9001
```

Tài khoản MinIO mặc định `lakeadmin / lakeadmin123` (đổi qua `.env`, xem `.env.example`).
Không có Docker: đặt `LAKE_BACKEND=local` để lake nằm ở thư mục `data/lake/`.

Kaggle API:
1. kaggle.com → Settings → API → **Create New Token**, đặt file `kaggle.json` vào `C:\Users\<tên>\.kaggle\`.
2. Vào trang **Rules** của hai cuộc thi và bấm *I Understand and Accept*:
   - https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/rules
   - https://www.kaggle.com/competitions/shopee-product-matching/rules

## Chạy

```bash
python run.py all                  # download → bronze → silver
python run.py download --only hm   # từng bước / từng nguồn
python run.py bronze
python run.py silver
python run.py gold                 # Gold Olist: galaxy schema 2 fact + 5 dim
python run.py olap                 # → data/reports/olist/olap.md
python run.py mine                 # → data/reports/olist/mining.md + hình
```

> Windows: luôn chạy trong env đã `conda activate btl` (hoặc `conda run -n btl python ...`).
> Gọi thẳng `envs\btl\python.exe` sẽ nạp nhầm DLL và matplotlib crash (0xc06d007f).

Mọi bước đều idempotent: chạy lại thay thế nguyên khối bảng cũ, không nhân bản dữ liệu.

## Hạ tầng Docker và nạp vào Data Warehouse

```bash
docker compose up -d --build        # MinIO, PostgreSQL 16 (+pgvector), Airflow 3, Superset
```

| Dịch vụ | Địa chỉ | Tài khoản (dev) |
|---|---|---|
| MinIO | http://localhost:9001 | lakeadmin / lakeadmin123 |
| Airflow | http://localhost:8080 | không cần đăng nhập (mọi người dùng là admin) |
| Superset | http://localhost:8088 | admin / admin — đã đăng ký sẵn kết nối "DW (PostgreSQL)" (role chỉ đọc `bi_reader`) |
| PostgreSQL | localhost:5432, database `dw` | dw / dw (ghi), bi_reader / bi_reader (chỉ đọc) |

Tài khoản kho dữ liệu nằm ở `data_warehouse/.env` (mẫu: `data_warehouse/.env.example`), nạp vào container
postgres/airflow/superset qua `env_file`; pipeline chạy trên máy cũng đọc file này.

### Khởi tạo Data Warehouse — `data_warehouse/scripts/`

Thư mục được mount làm `/docker-entrypoint-initdb.d` của Postgres, chạy **một lần** khi volume `pg-data` trống:

| File | Nội dung |
|---|---|
| `000_init_databases.sh` | role + database: `dw`, `airflow`, `superset`; role chỉ đọc cho BI; extension `vector` |
| `001_init_tables.sql` | schema `gold_hm` (staging), `hm` (star schema), `etl_audit.load_log`; quyền SELECT mặc định cho BI trên `hm`, `etl_audit` |
| `002_init_dim.sql` | `hm.dim_date`, `dim_category`, `dim_product` (SCD2), `dim_shop`, `dim_customer`, `dim_channel` — PK, UNIQUE, CHECK |
| `003_init_fact.sql` | `hm.fact_sales`, `hm.fact_product_daily` — phân vùng theo năm trên `date_key`, index, FK tới dim |
| `004_init_etl.sql` | thủ tục `CALL hm.build()`: staging → dim/fact trong một transaction, đối chiếu, gắn FK, làm mới MV |
| `005_init_superset.sql` | view phẳng `hm.v_sales`, `hm.v_product_daily`; materialized view `hm.mv_*` cho dashboard |

Volume đã có dữ liệu thì script không tự chạy lại: `docker compose down -v` (xoá mọi dữ liệu Postgres) hoặc
chạy tay `docker compose exec postgres psql -U postgres -f /docker-entrypoint-initdb.d/<file>.sql` (idempotent).

### Gold → Postgres

DAG **`gold_minio_to_postgres`** (Airflow → Trigger), hoặc `python run.py load --only hm`:

1. Mỗi bảng `s3://lake/gold/hm/<bảng>` được sao nguyên trạng sang staging `dw.gold_hm.<bảng>` (nạp vào bảng
   tạm, đổi tên trong một transaction), ghi `etl_audit.load_log`, đối chiếu số dòng và tổng revenue với lake.
2. `CALL hm.build()` dựng star schema `hm` từ staging bằng SQL; lệch số liệu → rollback, Superset vẫn thấy bản cũ.

`python run.py olap | mine --only hm` đọc star schema `hm` từ Postgres.

## Crawl Tiki tự động

Crawler `src/crawl/tiki.py` ghi `data/raw/tiki/<ngày UTC>/{listing,detail,sellers,reviews,categories}.jsonl.gz`,
mỗi ngày một snapshot listing; detail/seller làm mới sau 7 ngày, review khi `review_count` đổi
(`configs/config.yaml` → `crawl.tiki`). Chạy lại luôn tiếp tục từ chỗ dở (`data/raw/tiki/_state.json`).

```bash
python -m src.crawl.tiki --stages sellers --limit 5   # chạy thử
python -m src.crawl.tiki --forever                    # mỗi ngày một lượt; bị WAF chặn thì nghỉ 1 giờ rồi chạy lại
```

Hai cách chạy nền (chỉ dùng **một** cách, chạy song song sẽ bị chặn):

- **Airflow**: DAG `tiki_daily_crawl` lúc 02:00 hằng ngày, `listing_detail → sellers → reviews → bronze`.
- **Không có Docker** — systemd user service:
  ```bash
  cp deploy/tiki-crawl.service ~/.config/systemd/user/   # sửa đường dẫn nếu khác máy
  systemctl --user daemon-reload && systemctl --user enable --now tiki-crawl
  journalctl --user -u tiki-crawl -f                     # theo dõi
  python run.py bronze --only tiki                       # nạp vào bronze/tiki khi cần
  ```

## Cấu trúc lake (`s3://lake/` trên MinIO)

| Tầng | Nội dung | Quy tắc |
|---|---|---|
| `bronze/<nguồn>/<bảng>` | CSV → Parquet, mọi cột VARCHAR, + `_ingest_ts`, `_batch_id`, `_source_file` | Không sửa, không ép kiểu, không loại dòng. Lệch số dòng giữa các batch > 5% → cảnh báo (`batch_drift.csv`) |
| `silver/<nguồn>/<bảng>` | Ép kiểu, chuẩn hoá, khử trùng lặp, giao dịch đã lấy mẫu theo khách hàng | Bản ghi vi phạm → `silver/_quarantine/` kèm mã lỗi |
| `silver/sim/` | **Dữ liệu mô phỏng** cho H&M: `shop`, `product_shop`, `review`, `product_change` | Seed cố định; phân phối tham chiếu lấy từ Olist. Đối chiếu: `reports/hm/simulation.md` |
| `gold/hm/` | Bảng phẳng đã làm sạch, thống nhất tên/kiểu trường; bảng cho ML (`features_product`, `baskets`). Star schema dựng trong DW | Đối chiếu tự động với Silver sau mỗi lần dựng |

Mỗi bảng được ghi vào `.<tên>.tmp` rồi mới công bố (S3: sao chép phía server), nên người đọc
không bao giờ thấy bảng ghi dở; chạy lại thay thế nguyên khối.

### Gold H&M (lake) — bảng phẳng

Quy ước trường: khoá nghiệp vụ `*_id`, mã số `*_no`, ngày `*_date`, tên phân loại bỏ hậu tố `_name`.

| Bảng | Nội dung |
|---|---|
| `article` | một dòng một article (snapshot hiện tại), mã + tên các cấp phân loại |
| `article_change` | lịch sử đổi department (mô phỏng) — nguồn SCD2 ở DW |
| `customer`, `shop`, `article_shop` | khách hàng trong mẫu; 8 shop mô phỏng; article bán ở shop nào |
| `transaction` | một article, một giá, một kênh trong một giỏ `(customer_id, sale_date)`; `shop_id`, `basket_id`, `revenue`; phân vùng năm/tháng |
| `review` | review mô phỏng, điểm 1–5 |

### Star schema H&M (DW, schema `hm`)

| Bảng | Grain / nội dung |
|---|---|
| `fact_sales` | một article, một giá, một kênh trong một giỏ; `basket_id` (degenerate) |
| `fact_product_daily` | một article tại một shop trong một ngày (trong thời gian article có bán): `list_price`, `stock_qty`, `rating_avg`, `review_count` (snapshot) + `daily_sales_qty` |
| `dim_product` | SCD Type 2 (`valid_from`, `valid_to`, `is_current`); giao dịch nối vào phiên bản đúng tại ngày mua |
| `dim_category` | department → section → index → index_group |
| `dim_shop` | 8 shop mô phỏng, `node_id` để phân mảnh |
| `dim_date`, `dim_customer` (+ `rfm_segment`), `dim_channel` | |
| `v_sales`, `v_product_daily`, `mv_*` | lớp cho Superset: view phẳng fact ⨝ dim, khối con vật chất hoá |

Báo cáo trong `data/reports/`: `missing_values.csv` (tỷ lệ thiếu theo cột, Bronze & Silver),
`quality_log.csv` (số bản ghi hợp lệ / quarantine theo mã lỗi), `profile_silver.md`.

## Quyết định xử lý dữ liệu (ghi vào báo cáo)

- **Lấy mẫu theo khách hàng**: 100.000 khách hàng, chọn tất định bằng `md5(customer_id || seed)`,
  giữ *toàn bộ* giao dịch của họ trên cả giai đoạn 09/2018–09/2020 → giỏ hàng nguyên vẹn.
- **Dòng giao dịch trùng hệt** trong H&M là mua nhiều đơn vị → gộp thành `sales_qty`, không xoá.
- **Giỏ hàng** = `(customer_id, t_dat)`.
- `article_id` chuẩn hoá về 10 ký tự (bù số 0 đầu), khớp tên file ảnh.
- Biến phân loại thiếu → `'Unknown'`; tuổi ngoài [16, 100] → NULL (giữ khách hàng).
- `price` của H&M đã được chuẩn hoá bởi nhà cung cấp, không phải đơn vị tiền tệ thật.
- Olist review: `review_id` lặp → giữ bản có `review_answer_timestamp` mới nhất.
- **Shop và rating của H&M là dữ liệu sinh** (§4.3 đề cương): 8 shop có trọng số = tỷ trọng sản phẩm của
  8 nhóm seller Olist; mỗi article ở 1–3 shop; mỗi dòng giao dịch chọn một shop theo hash của giỏ hàng;
  điểm review sinh theo mô hình thứ bậc và hiệu chỉnh cho phân phối 1–5 sao khớp Olist.
- **SCD Type 2**: ~2% article có một lần đổi department mô phỏng; giá trị hiện tại là snapshot thật của H&M.

| Mã lỗi | Ý nghĩa |
|---|---|
| `A_ID_FORMAT`, `A_PRODUCT_CODE` | article_id không đủ 10 chữ số / thiếu product_code |
| `C_ID_FORMAT` | customer_id không phải hash 64 ký tự |
| `T_DATE`, `T_PRICE`, `T_CHANNEL` | ngày không parse được / giá ≤ 0 / kênh ∉ {1,2} |
| `T_FK_ARTICLE`, `T_FK_CUSTOMER` | vi phạm toàn vẹn tham chiếu |
| `OI_PRICE`, `OI_FK_SELLER`, `R_SCORE` | Olist: giá ≤ 0 / seller không tồn tại / điểm ∉ [1,5] |
| `SP_IMAGE`, `SP_LABEL` | Shopee: thiếu ảnh / thiếu label_group |
