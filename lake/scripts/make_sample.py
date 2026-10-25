import argparse
import shutil
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]

CATEGORIES = ["tops", "bottoms", "outerwear", "footwear"]
# Số ảnh tối thiểu để một sản phẩm đủ điều kiện: H&M/Shopee chỉ có 1 ảnh, brands 1-2 ảnh, còn lại từ 3 ảnh.
MIN_IMAGES = {"zara": 3, "tiki": 3, "uniqlo": 3, "brands": 2, "hm": 1, "shopee": 1}
MAX_IMAGES = 4


def gold(lake: Path, table: str) -> str:
    return f"read_parquet('{(lake / 'gold' / table).as_posix()}/**/*.parquet', union_by_name=true)"


def pick_products(con, lake: Path, per_source: int, seed: str) -> None:
    per_cat = per_source // len(CATEGORIES)
    cats = ",".join(f"'{c}'" for c in CATEGORIES)
    mins = " ".join(f"when '{s}' then {n}" for s, n in MIN_IMAGES.items())
    con.execute(f"""
        create temp table eligible as
        with img as (
            select product_key, count(*) n from {gold(lake, 'fact_media')}
            where media_type = 'image' and download_status = 'ok' group by 1
        )
        select p.product_key, p.source, p.category_l1,
               hash(p.product_key || '{seed}') h
        from {gold(lake, 'dim_product')} p join img using (product_key)
        where p.category_l1 in ({cats})
          and p.source in ({",".join(f"'{s}'" for s in MIN_IMAGES)})
          and img.n >= case p.source {mins} end
    """)
    con.execute(f"""
        create temp table chosen as
        with ranked as (
            select *, row_number() over (partition by source, category_l1 order by h) rc
            from eligible
        ),
        base as (select * from ranked where rc <= {per_cat}),
        fill as (
            select *, row_number() over (partition by source order by h) rf
            from ranked where rc > {per_cat}
        )
        select product_key, source, category_l1 from base
        union all
        select f.product_key, f.source, f.category_l1 from fill f
        join (select source, count(*) n from base group by 1) b using (source)
        where f.rf <= {per_source} - b.n
    """)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, default=ROOT / "data")
    ap.add_argument("--per-source", type=int, default=100)
    ap.add_argument("--seed", default="sample-v1")
    ap.add_argument("--out", type=Path)
    a = ap.parse_args()

    lake = a.data / "lakehouse"
    out = a.out or a.data / "sample"
    if out.exists():
        shutil.rmtree(out)
    (out / "images").mkdir(parents=True)

    con = duckdb.connect()
    pick_products(con, lake, a.per_source, a.seed)
    con.execute(f"""
        create temp table media as
        select m.*, row_number() over (partition by m.product_key order by m.position, m.media_key) rn
        from {gold(lake, 'fact_media')} m join chosen using (product_key)
        where m.media_type = 'image' and m.download_status = 'ok'
        qualify rn <= {MAX_IMAGES}
    """)

    rows = con.execute("select media_key, source, local_path from media").fetchall()
    new_path, missing = {}, []
    for key, source, lp in rows:
        src = Path(lp) if Path(lp).is_absolute() else a.data / "lakehouse" / lp
        if not src.is_file():
            missing.append(key)
            continue
        dst = out / "images" / source / src.name
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        new_path[key] = f"images/{source}/{src.name}"
    con.execute("create temp table newpath(media_key varchar, path varchar)")
    con.executemany("insert into newpath values (?, ?)", list(new_path.items()))

    con.execute(f"""
        copy (select m.* exclude (local_path, rn), n.path as local_path
              from media m join newpath n using (media_key) order by m.source, m.product_key, m.position)
        to '{(out / "fact_media.parquet").as_posix()}' (format parquet)
    """)
    con.execute(f"""
        copy (select p.* from {gold(lake, 'dim_product')} p
              where p.product_key in (select product_key from media where media_key in (select media_key from newpath))
              order by p.source, p.category_l1, p.product_key)
        to '{(out / "dim_product.parquet").as_posix()}' (format parquet)
    """)
    con.execute(f"""
        copy (select s.* from {gold(lake, 'fact_price_snapshot')} s
              where s.product_key in (select product_key from read_parquet('{(out / "dim_product.parquet").as_posix()}'))
              order by s.product_key, s.date_key)
        to '{(out / "fact_price_snapshot.parquet").as_posix()}' (format parquet)
    """)
    for table, key in [("dim_category", "category_key"), ("dim_brand", "brand_key")]:
        con.execute(f"""
            copy (select t.* from {gold(lake, table)} t
                  where t.{key} in (select {key} from read_parquet('{(out / "dim_product.parquet").as_posix()}')))
            to '{(out / f"{table}.parquet").as_posix()}' (format parquet)
        """)

    p = (out / "dim_product.parquet").as_posix()
    mp = (out / "fact_media.parquet").as_posix()
    print(f"ảnh thiếu file: {len(missing)}")
    print(con.execute(f"""
        select p.source, count(*) sp,
               sum((p.category_l1='tops')::int) tops, sum((p.category_l1='bottoms')::int) bottoms,
               sum((p.category_l1='outerwear')::int) outerwear, sum((p.category_l1='footwear')::int) footwear,
               (select count(*) from read_parquet('{mp}') m where m.source = p.source) anh
        from read_parquet('{p}') p group by 1 order by 1
    """).df().to_string(index=False))


if __name__ == "__main__":
    main()
