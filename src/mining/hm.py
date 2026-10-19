from __future__ import annotations

import time

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.cluster import AgglomerativeClustering, KMeans
from sklearn.metrics import davies_bouldin_score, silhouette_score
from sklearn.preprocessing import StandardScaler

from src.common.config import connect, get_logger, load_config, paths
from src.mining.olist import _md
from src.warehouse.load import dw_views

log = get_logger("mining.hm")
SEED = load_config()["seed"]
OUT = paths().reports / "hm"
SPLIT_DATE = "2020-04-01"


def _con():
    con = connect()
    dw_views(con, "hm", ["dim_date", "dim_product", "dim_customer", "dim_channel"])
    con.execute("CREATE OR REPLACE TEMP TABLE fact_sales AS SELECT * FROM pg.hm.fact_sales")
    return con


def _baskets(con, level: str) -> pd.DataFrame:
    return con.sql(f"""
        SELECT DISTINCT f.basket_id, p.{level} AS item, d.date >= DATE '{SPLIT_DATE}' AS is_test
        FROM fact_sales f JOIN dim_product p USING (product_key) JOIN dim_date d USING (date_key)
    """).df()


def _onehot(pairs: pd.DataFrame, items: list[str] | None = None) -> pd.DataFrame:
    if items is None:
        items = sorted(pairs["item"].unique())
    col = {it: i for i, it in enumerate(items)}
    pairs = pairs[pairs["item"].isin(col)]
    b_codes, b_uni = pd.factorize(pairs["basket_id"])
    m = sparse.csr_matrix((np.ones(len(pairs), dtype=bool), (b_codes, pairs["item"].map(col).to_numpy())),
                          shape=(len(b_uni), len(items)))
    return pd.DataFrame.sparse.from_spmatrix(m, columns=items)


def _rules(X: pd.DataFrame, min_support: float, n_baskets_total: int) -> pd.DataFrame:
    from mlxtend.frequent_patterns import association_rules, fpgrowth
    # support tính trên mọi giỏ: X chỉ chứa giỏ có item thuộc tập lọc → hiệu chỉnh mẫu số
    scale = len(X) / n_baskets_total
    fi = fpgrowth(X, min_support=min_support / scale, use_colnames=True)
    fi["support"] *= scale
    if fi.empty or fi["itemsets"].apply(len).max() < 2:
        return pd.DataFrame()
    return association_rules(fi, metric="lift", min_threshold=0.0)


def _eval_on(rules: pd.DataFrame, X: pd.DataFrame, n_total: int) -> pd.DataFrame:
    cols = {c: X[c].sparse.to_dense().to_numpy() if c in X else np.zeros(len(X), bool) for c in
            set().union(*rules["antecedents"], *rules["consequents"])}
    out = []
    for a, c in zip(rules["antecedents"], rules["consequents"]):
        ma = np.logical_and.reduce([cols[i] for i in a])
        mc = np.logical_and.reduce([cols[i] for i in c])
        sa, sc, sac = ma.sum() / n_total, mc.sum() / n_total, (ma & mc).sum() / n_total
        conf = sac / sa if sa else np.nan
        out.append((sac, conf, conf / sc if sc else np.nan))
    return pd.DataFrame(out, columns=["test_support", "test_confidence", "test_lift"], index=rules.index)


def _one_direction(r: pd.DataFrame) -> pd.DataFrame:
    key = [frozenset(a | c) for a, c in zip(r["antecedents"], r["consequents"])]
    return r.assign(_k=key).sort_values("confidence", ascending=False).drop_duplicates("_k").drop(columns="_k")


def _held(r: pd.DataFrame) -> str:
    ev = r["test_lift"].notna()
    if not ev.any():
        return "không luật nào đo được trên tập kiểm tra"
    return (f"{ev.sum()} luật đo được trên tập kiểm tra, trong đó "
            f"{100 * (r.loc[ev, 'test_lift'] > 1).mean():.1f}% vẫn có lift > 1")


def _fmt_rules(r: pd.DataFrame, names: dict | None = None, n_total: int | None = None) -> pd.DataFrame:
    lab = (lambda s: ", ".join(names.get(i, i) for i in sorted(s))) if names else (lambda s: ", ".join(sorted(s)))
    return pd.DataFrame({"luật": r["antecedents"].apply(lab) + " → " + r["consequents"].apply(lab),
                         "support": r["support"], "confidence": r["confidence"], "lift": r["lift"],
                         "test lift": r["test_lift"],
                         **({"giỏ (train)": (r["support"] * n_total).round().astype(int)} if n_total else {})})


def association(con) -> list[str]:
    lines = ["## 1. Luật kết hợp", ""]
    n_tr, n_te = con.execute(f"""
        SELECT count(DISTINCT basket_id) FILTER (WHERE d.date < DATE '{SPLIT_DATE}'),
               count(DISTINCT basket_id) FILTER (WHERE d.date >= DATE '{SPLIT_DATE}')
        FROM fact_sales f JOIN dim_date d USING (date_key)""").fetchone()
    lines += [f"- Giỏ = (customer_id, t_dat). Huấn luyện: trước {SPLIT_DATE} ({n_tr:,} giỏ); "
              f"kiểm tra: từ {SPLIT_DATE} ({n_te:,} giỏ).", ""]

    from mlxtend.frequent_patterns import apriori, fpgrowth
    pt = _baskets(con, "product_type")
    X = _onehot(pt[~pt.is_test]).sparse.to_dense()   # 131 cột: dense vẫn nhỏ
    timing = []
    for ms in [0.02, 0.01, 0.005, 0.0025]:
        t0 = time.perf_counter(); a = apriori(X, min_support=ms, use_colnames=True, low_memory=True)
        ta = time.perf_counter() - t0
        t0 = time.perf_counter(); f = fpgrowth(X, min_support=ms, use_colnames=True)
        tf = time.perf_counter() - t0
        same = set(map(frozenset, a["itemsets"])) == set(map(frozenset, f["itemsets"]))
        timing.append({"min_support": ms, "itemsets": len(f), "Apriori (s)": ta, "FP-Growth (s)": tf,
                       "trùng khớp": "có" if same else "KHÔNG"})
        log.info("  min_support %.4f: %d itemsets, apriori %.2fs, fpgrowth %.2fs", ms, len(f), ta, tf)
    lines += ["### Apriori vs FP-Growth (mức product_type, tập huấn luyện)", "",
              _md(pd.DataFrame(timing), "{:.4f}"), ""]

    for level, ms in [("product_group", 0.01), ("product_type", 0.005)]:
        pairs = pt if level == "product_type" else _baskets(con, level)
        items = sorted(pairs["item"].unique())
        Xtr, Xte = _onehot(pairs[~pairs.is_test], items), _onehot(pairs[pairs.is_test], items)
        r = _rules(Xtr, ms, n_tr)
        r = _one_direction(r[r["lift"] > 1.2].copy())
        r = r.join(_eval_on(r, Xte, n_te))
        show = r if level == "product_group" else \
            r[(r["antecedents"].apply(len) == 1) & (r["consequents"].apply(len) == 1)]
        lines += [f"### Mức {level} (min_support = {ms})", "",
                  f"- {len(r)} luật có lift > 1,2 trên tập huấn luyện (mỗi cặp tính một chiều); {_held(r)}.", "",
                  _md(_fmt_rules(show.sort_values("lift", ascending=False).head(8))), ""]
        log.info("  %s: %d luật", level, len(r))

    art = _baskets(con, "article_id")
    min_cnt = 50
    cnt = art[~art.is_test]["item"].value_counts()
    items = sorted(cnt[cnt >= min_cnt].index)
    Xtr, Xte = _onehot(art[~art.is_test], items), _onehot(art[art.is_test], items)
    r = _rules(Xtr, min_cnt / n_tr, n_tr)
    r = _one_direction(r[(r["lift"] > 1.2) & (r["antecedents"].apply(len) == 1)
                         & (r["consequents"].apply(len) == 1)].copy())
    prod = con.sql("SELECT article_id, product_code, prod_name, colour FROM dim_product WHERE is_current"
                   ).df().set_index("article_id")
    a1 = r["antecedents"].apply(lambda s: next(iter(s)))
    c1 = r["consequents"].apply(lambda s: next(iter(s)))
    same_code = (prod.loc[a1, "product_code"].to_numpy() == prod.loc[c1, "product_code"].to_numpy())
    r = r.join(_eval_on(r, Xte, n_te))
    names = {i: f"{prod.at[i, 'prod_name']} ({prod.at[i, 'colour']})" for i in set(a1) | set(c1)}
    top = r.sort_values("support", ascending=False).head(8)
    lines += [f"### Mức article (min_support = {min_cnt} giỏ)", "",
              f"- {len(items):,} article có ≥ {min_cnt} giỏ trong tập huấn luyện.",
              f"- {len(r)} luật 1→1 có lift > 1,2 (mỗi cặp tính một chiều); {_held(r)}.",
              f"- {100 * same_code.mean():.1f}% số luật nối hai article cùng product_code.", "",
              _md(_fmt_rules(top, names, n_tr)), ""]
    log.info("  article: %d luật", len(r))
    return lines


CUST_SQL = """
    WITH b AS (
        SELECT f.customer_key, f.basket_id, min(d.date) AS date, count(DISTINCT f.product_key) AS items
        FROM fact_sales f JOIN dim_date d USING (date_key) GROUP BY 1, 2
    ), d_max AS (SELECT max(date) AS d_max FROM b),
    per_basket AS (
        SELECT customer_key, max(date) AS last_date, count(*) AS n_baskets, avg(items) AS items_per_basket
        FROM b GROUP BY 1
    ), per_line AS (
        SELECT customer_key, sum(revenue) AS revenue, sum(sales_qty) AS units,
               sum(revenue) / sum(sales_qty) AS avg_price, avg((channel_key = 2)::INT) AS channel2_share
        FROM fact_sales GROUP BY 1
    )
    SELECT pb.customer_key, date_diff('day', pb.last_date, (SELECT d_max FROM d_max)) AS recency_days,
           pb.n_baskets, pb.items_per_basket, pl.revenue, pl.units, pl.avg_price, pl.channel2_share, c.age
    FROM per_basket pb JOIN per_line pl USING (customer_key) JOIN dim_customer c USING (customer_key)
"""
FEATS = ["recency_days", "n_baskets", "items_per_basket", "revenue", "units", "avg_price",
         "channel2_share", "age"]
LOG_FEATS = ["n_baskets", "items_per_basket", "revenue", "units"]


def cluster_customers(con) -> list[str]:
    df = con.sql(CUST_SQL).df()
    df["age"] = df["age"].fillna(df["age"].median())
    Z = df[FEATS].copy()
    Z[LOG_FEATS] = np.log1p(Z[LOG_FEATS])
    Z = StandardScaler().fit_transform(Z)
    rng = np.random.default_rng(SEED)
    sample = rng.choice(len(Z), size=10_000, replace=False)

    scores = []
    for k in range(2, 9):
        km = KMeans(n_clusters=k, n_init=5, random_state=SEED).fit(Z)
        ward = AgglomerativeClustering(n_clusters=k, linkage="ward").fit(Z[sample])
        scores.append({"k": k,
                       "silhouette K-Means": silhouette_score(Z, km.labels_, sample_size=10_000, random_state=SEED),
                       "DBI K-Means": davies_bouldin_score(Z, km.labels_),
                       "silhouette Ward (mẫu)": silhouette_score(Z[sample], ward.labels_),
                       "DBI Ward (mẫu)": davies_bouldin_score(Z[sample], ward.labels_)})
        log.info("  k=%d", k)
    sc = pd.DataFrame(scores)
    k_best = int(sc[sc.k.between(3, 6)].sort_values("silhouette K-Means", ascending=False).iloc[0].k)
    df["cluster"] = KMeans(n_clusters=k_best, n_init=5, random_state=SEED).fit_predict(Z)

    prof = df.groupby("cluster").agg(
        customers=("customer_key", "size"), baskets_median=("n_baskets", "median"),
        items_per_basket=("items_per_basket", "mean"), recency_median=("recency_days", "median"),
        avg_price=("avg_price", "mean"), channel2_share=("channel2_share", "mean"),
        age_median=("age", "median")).reset_index()
    prof["revenue_share"] = df.groupby("cluster")["revenue"].sum().values / df["revenue"].sum()
    return ["## 2. Phân cụm khách hàng", "",
            f"- {len(df):,} khách hàng; {len(FEATS)} đặc trưng (log1p cho {', '.join(LOG_FEATS)}; z-score; "
            f"age thiếu → trung vị). Silhouette K-Means tính trên mẫu 10.000; Ward chạy trên mẫu 10.000.", "",
            _md(sc), "", f"Chọn **k = {k_best}** (silhouette K-Means cao nhất trong khoảng 3–6).", "",
            _md(prof, "{:.3f}"), ""]


def run() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = _con()
    md = ["# Khai phá dữ liệu — H&M (mẫu 100.000 khách hàng)", ""]
    md += association(con)
    md += cluster_customers(con)
    (OUT / "mining.md").write_text("\n".join(md), encoding="utf-8")
    log.info("Đã ghi %s", OUT / "mining.md")
