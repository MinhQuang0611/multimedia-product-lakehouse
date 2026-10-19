\set ON_ERROR_STOP on
\getenv dw_db DW_DBNAME
\getenv dw_user DW_USER
\connect :"dw_db"
SET ROLE :"dw_user";

CREATE TABLE IF NOT EXISTS hm.dim_date (
    date_key     integer  PRIMARY KEY,                 -- yyyymmdd
    date         date     NOT NULL UNIQUE,
    day          smallint NOT NULL,
    month        smallint NOT NULL,
    year_month   char(7)  NOT NULL,                    -- 2019-06
    quarter      smallint NOT NULL,
    year_quarter char(7)  NOT NULL,                    -- 2019-Q2
    year         smallint NOT NULL,
    weekday      smallint NOT NULL,                    -- ISO: 1 = thứ Hai … 7 = Chủ nhật
    is_weekend   boolean  NOT NULL
);

-- department → section → index → index_group (snowflake từ dim_product)
CREATE TABLE IF NOT EXISTS hm.dim_category (
    category_key   integer PRIMARY KEY,                -- = department_no
    department     text    NOT NULL,
    section_no     integer NOT NULL,
    section        text    NOT NULL,
    index_code     text    NOT NULL,
    index_name     text    NOT NULL,
    index_group_no integer NOT NULL,
    index_group    text    NOT NULL
);

-- SCD Type 2: một dòng một PHIÊN BẢN article; giao dịch nối vào phiên bản đúng tại ngày mua.
CREATE TABLE IF NOT EXISTS hm.dim_product (
    product_key   integer     PRIMARY KEY,
    article_id    varchar(10) NOT NULL,
    product_code  varchar(7)  NOT NULL,
    prod_name     text        NOT NULL,
    product_type  text        NOT NULL,
    product_group text        NOT NULL,
    garment_group text        NOT NULL,
    colour        text        NOT NULL,
    appearance    text        NOT NULL,
    category_key  integer     NOT NULL REFERENCES hm.dim_category,
    valid_from    date        NOT NULL,
    valid_to      date        NOT NULL,
    is_current    boolean     NOT NULL,
    UNIQUE (article_id, valid_from),
    CHECK (valid_from <= valid_to)
);
CREATE UNIQUE INDEX IF NOT EXISTS dim_product_current_uq ON hm.dim_product (article_id) WHERE is_current;
CREATE INDEX IF NOT EXISTS dim_product_category_idx ON hm.dim_product (category_key);

-- 8 shop MÔ PHỎNG; node_id dùng để phân mảnh
CREATE TABLE IF NOT EXISTS hm.dim_shop (
    shop_key     smallint PRIMARY KEY,
    shop_id      text     NOT NULL UNIQUE,
    shop_name    text     NOT NULL,
    size_rank    smallint NOT NULL,
    olist_weight double precision NOT NULL,            -- trọng số lấy từ phân phối seller Olist
    node_id      text     NOT NULL
);

CREATE TABLE IF NOT EXISTS hm.dim_customer (
    customer_key           integer     PRIMARY KEY,
    customer_id            varchar(64) NOT NULL UNIQUE,
    age                    smallint,                   -- NULL: thiếu hoặc ngoài [16, 100]
    age_band               text        NOT NULL,
    club_member_status     text        NOT NULL,
    fashion_news_frequency text        NOT NULL,
    rfm_segment            text                        -- ghi ngược từ phân khúc RFM (Tầng 3)
);

-- sales_channel_id (1, 2) — nguồn không công bố ý nghĩa
CREATE TABLE IF NOT EXISTS hm.dim_channel (
    channel_key smallint PRIMARY KEY,
    channel     text     NOT NULL
);
