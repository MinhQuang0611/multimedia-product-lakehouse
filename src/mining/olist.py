from __future__ import annotations

import time

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering, KMeans
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import (average_precision_score, confusion_matrix, davies_bouldin_score,
                             f1_score, precision_score, recall_score, roc_auc_score,
                             silhouette_score)
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier, export_text

from src.common.config import connect, get_logger, load_config, paths
from src.common.lake import scan

log = get_logger("mining.olist")
SEED = load_config()["seed"]
OUT = paths().reports / "olist"


def _con():
    con = connect()
    g = paths().gold / "olist"
    for t in ["fact_sales", "fact_order", "dim_date", "dim_customer", "dim_seller",
              "dim_product", "dim_payment"]:
        con.execute(f"CREATE OR REPLACE TEMP VIEW {t} AS SELECT * FROM {scan(g / t)}")
    return con


def _md(df: pd.DataFrame, floatfmt: str = "{:.3f}") -> str:
    cols = list(df.columns)
    fmt = lambda v: floatfmt.format(v) if isinstance(v, (float, np.floating)) else str(v)
    return "\n".join(["| " + " | ".join(map(str, cols)) + " |", "|" + "---|" * len(cols),
                      *["| " + " | ".join(fmt(v) for v in r) + " |" for r in df.itertuples(index=False)]])


REVIEW_SQL = """
    SELECT f.order_id, d.date AS purchase_date, (f.review_score <= 2)::INT AS bad_review,
           f.delay_days, f.delivery_days, f.promised_days, f.to_carrier_days, f.approve_days,
           f.n_items, f.n_products, f.n_sellers, f.items_value, f.freight_value,
           f.freight_value / nullif(f.items_value, 0) AS freight_ratio,
           f.same_state::INT AS same_state,
           p.photos_qty, p.description_length, p.weight_g, p.volume_cm3, p.category,
           c.region AS c_region, s.region AS s_region,
           pm.payment_type, pm.installment_band
    FROM fact_order f
    JOIN dim_date d ON d.date_key = f.purchase_date_key
    JOIN dim_customer c USING (customer_key)
    LEFT JOIN dim_seller s ON s.seller_key = f.main_seller_key
    LEFT JOIN dim_product p ON p.product_key = f.main_product_key
    LEFT JOIN dim_payment pm USING (payment_key)
    WHERE f.order_status = 'delivered' AND f.review_score IS NOT NULL AND f.delivery_days IS NOT NULL
"""
CATEGORICAL = ["category", "c_region", "s_region", "payment_type", "installment_band"]
SPLIT_DATE = "2018-04-01"   # chia theo thời gian: học trên quá khứ, kiểm thử trên tương lai


def _metrics(name, y, pred, score=None) -> dict:
    tn, fp, fn, tp = confusion_matrix(y, pred, labels=[0, 1]).ravel()
    return {"mô hình": name, "precision": precision_score(y, pred, zero_division=0),
            "recall": recall_score(y, pred), "F1": f1_score(y, pred),
            "ROC-AUC": roc_auc_score(y, score) if score is not None else float("nan"),
            "PR-AUC": average_precision_score(y, score) if score is not None else float("nan"),
            "TP": tp, "FP": fp, "FN": fn, "TN": tn}


def _best_threshold(y, score) -> float:
    grid = np.quantile(score, np.linspace(0.5, 0.99, 60))
    return float(max(grid, key=lambda t: f1_score(y, score >= t)))


def classify_reviews(con) -> list[str]:
    df = con.sql(REVIEW_SQL).df()
    top = df["category"].value_counts().index[:30]
    df["category"] = df["category"].where(df["category"].isin(top), "other")
    X = pd.get_dummies(df.drop(columns=["order_id", "purchase_date", "bad_review"]),
                       columns=CATEGORICAL, dummy_na=True, dtype=float)
    y = df["bad_review"].to_numpy()

    is_test = (df["purchase_date"] >= pd.Timestamp(SPLIT_DATE)).to_numpy()
    tr_idx = np.where(~is_test)[0]
    # 20% cuối (theo thời gian) của tập huấn luyện làm tập kiểm định để chọn ngưỡng
    order = tr_idx[np.argsort(df["purchase_date"].to_numpy()[tr_idx], kind="stable")]
    fit_idx, val_idx = order[: int(0.8 * len(order))], order[int(0.8 * len(order)):]
    te_idx = np.where(is_test)[0]
    Xf = X.fillna(-1)

    rows = [
        _metrics("Cơ sở 1: luôn dự đoán 'tốt'", y[te_idx], np.zeros(len(te_idx), int)),
        _metrics("Cơ sở 2: luật 'giao trễ → xấu'", y[te_idx],
                 (df["delay_days"].to_numpy()[te_idx] > 0).astype(int),
                 df["delay_days"].to_numpy()[te_idx]),
    ]

    tree = DecisionTreeClassifier(max_depth=5, min_samples_leaf=200, class_weight="balanced",
                                  random_state=SEED).fit(Xf.iloc[fit_idx], y[fit_idx])
    t_tree = _best_threshold(y[val_idx], tree.predict_proba(Xf.iloc[val_idx])[:, 1])
    s_tree = tree.predict_proba(Xf.iloc[te_idx])[:, 1]
    rows.append(_metrics("Cây quyết định (depth 5)", y[te_idx], (s_tree >= t_tree).astype(int), s_tree))

    hgb = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, random_state=SEED)
    hgb.fit(X.iloc[fit_idx], y[fit_idx])
    t_hgb = _best_threshold(y[val_idx], hgb.predict_proba(X.iloc[val_idx])[:, 1])
    s_hgb = hgb.predict_proba(X.iloc[te_idx])[:, 1]
    rows.append(_metrics("Gradient Boosting", y[te_idx], (s_hgb >= t_hgb).astype(int), s_hgb))

    res = pd.DataFrame(rows)
    imp = (pd.Series(tree.feature_importances_, index=X.columns).sort_values(ascending=False)
           .head(10).rename("importance").reset_index().rename(columns={"index": "đặc trưng"}))
    rules = export_text(tree, feature_names=list(X.columns), max_depth=3, decimals=1)

    best = res.iloc[2:].sort_values("PR-AUC", ascending=False).iloc[0]
    cm = np.array([[best["TN"], best["FP"]], [best["FN"], best["TP"]]])
    fig, ax = plt.subplots(figsize=(4, 3.4))
    ax.imshow(cm, cmap="Blues")
    for (i, j), v in np.ndenumerate(cm):
        ax.text(j, i, f"{v:,}", ha="center", va="center", color="black")
    ax.set_xticks([0, 1], ["dự đoán tốt", "dự đoán xấu"]); ax.set_yticks([0, 1], ["thực tế tốt", "thực tế xấu"])
    ax.set_title(best["mô hình"]); fig.tight_layout(); fig.savefig(OUT / "confusion_matrix.png", dpi=130)
    plt.close(fig)

    log.info("  phân loại: %s train / %s test, tỷ lệ xấu test %.1f%%",
             f"{len(tr_idx):,}", f"{len(te_idx):,}", 100 * y[te_idx].mean())
    return [
        "## 1. Phân loại — dự báo đánh giá xấu (≤ 2★)", "",
        f"- Mẫu: {len(df):,} đơn đã giao có review. Nhãn dương (xấu): {100 * y.mean():.1f}%.",
        f"- Chia theo thời gian: huấn luyện trước {SPLIT_DATE} ({len(tr_idx):,} đơn, 20% cuối dùng chọn ngưỡng), "
        f"kiểm thử từ {SPLIT_DATE} ({len(te_idx):,} đơn, {100 * y[te_idx].mean():.1f}% xấu).",
        f"- Ngưỡng quyết định chọn theo F1 trên tập kiểm định: cây {t_tree:.3f}, GB {t_hgb:.3f}.", "",
        _md(res), "", "Mười đặc trưng quan trọng nhất của cây quyết định:", "", _md(imp), "",
        "Ba tầng đầu của cây:", "", "```", rules, "```", "",
    ]


SELLER_SQL = """
    WITH s AS (
        SELECT seller_key, sum(price) AS revenue, count(DISTINCT order_id) AS n_orders,
               count(DISTINCT product_key) AS n_products, avg(price) AS avg_price,
               sum(freight_value) / sum(price) AS freight_ratio,
               count(DISTINCT p.category) AS n_categories,
               date_diff('month', min(d.date), max(d.date)) + 1 AS active_months
        FROM fact_sales f JOIN dim_product p USING (product_key) JOIN dim_date d USING (date_key)
        WHERE order_status = 'delivered' GROUP BY seller_key
    ), q AS (
        SELECT main_seller_key AS seller_key, avg(review_score) AS avg_review,
               avg((review_score <= 2)::INT) AS bad_rate, avg((delay_days > 0)::INT) AS late_rate,
               avg(to_carrier_days) AS to_carrier_days
        FROM fact_order WHERE order_status = 'delivered' GROUP BY 1
    )
    SELECT * FROM s JOIN q USING (seller_key) WHERE n_orders >= 10
"""
SELLER_FEATS = ["revenue", "n_orders", "n_products", "avg_price", "freight_ratio", "n_categories",
                "active_months", "avg_review", "bad_rate", "late_rate", "to_carrier_days"]
LOG_FEATS = ["revenue", "n_orders", "n_products", "avg_price"]


def cluster_sellers(con) -> list[str]:
    df = con.sql(SELLER_SQL).df().dropna()
    Z = df[SELLER_FEATS].copy()
    Z[LOG_FEATS] = np.log1p(Z[LOG_FEATS])
    Z = StandardScaler().fit_transform(Z)

    scores = []
    for k in range(2, 9):
        km = KMeans(n_clusters=k, n_init=10, random_state=SEED).fit(Z)
        ag = AgglomerativeClustering(n_clusters=k, linkage="ward").fit(Z)
        scores.append({"k": k, "silhouette K-Means": silhouette_score(Z, km.labels_),
                       "DBI K-Means": davies_bouldin_score(Z, km.labels_),
                       "silhouette Ward": silhouette_score(Z, ag.labels_),
                       "DBI Ward": davies_bouldin_score(Z, ag.labels_)})
    sc = pd.DataFrame(scores)
    # k = 2 thường thắng silhouette nhưng quá thô để hành động → chọn k tốt nhất trong [3, 6]
    k_best = int(sc[sc.k.between(3, 6)].sort_values("silhouette K-Means", ascending=False).iloc[0].k)
    df["cluster"] = KMeans(n_clusters=k_best, n_init=10, random_state=SEED).fit_predict(Z)

    prof = df.groupby("cluster").agg(
        sellers=("seller_key", "size"), revenue_median=("revenue", "median"),
        orders_median=("n_orders", "median"), avg_price=("avg_price", "median"),
        categories=("n_categories", "median"), avg_review=("avg_review", "mean"),
        bad_rate=("bad_rate", "mean"), late_rate=("late_rate", "mean"),
        to_carrier_days=("to_carrier_days", "mean")).reset_index()
    prof["revenue_share"] = df.groupby("cluster")["revenue"].sum().values / df["revenue"].sum()

    fig, ax = plt.subplots(figsize=(5, 3.2))
    ax.plot(sc.k, sc["silhouette K-Means"], "o-", label="K-Means")
    ax.plot(sc.k, sc["silhouette Ward"], "s--", label="Ward")
    ax.axvline(k_best, color="grey", lw=0.8); ax.set_xlabel("k"); ax.set_ylabel("Silhouette")
    ax.legend(); fig.tight_layout(); fig.savefig(OUT / "seller_silhouette.png", dpi=130); plt.close(fig)

    log.info("  phân cụm: %s người bán, k=%d", f"{len(df):,}", k_best)
    return [
        "## 2. Phân cụm người bán", "",
        f"- {len(df):,} người bán có ≥ 10 đơn đã giao; {len(SELLER_FEATS)} đặc trưng "
        f"(log1p cho {', '.join(LOG_FEATS)}; chuẩn hoá z-score).", "",
        _md(sc), "", f"Chọn **k = {k_best}** (silhouette K-Means cao nhất trong khoảng 3–6).", "",
        _md(prof, "{:.2f}"), "",
    ]


def association(con) -> list[str]:
    from mlxtend.frequent_patterns import apriori, association_rules, fpgrowth
    from mlxtend.preprocessing import TransactionEncoder

    lines = ["## 3. Luật kết hợp — kiểm tra khả thi", ""]
    for level, key, only_multi in [("đơn hàng", "f.order_id", False),
                                   ("khách hàng (gộp mọi đơn)", "f.customer_key", False),
                                   ("khách hàng, chỉ giỏ ≥ 2 danh mục", "f.customer_key", True)]:
        baskets = con.sql(f"""
            SELECT list(DISTINCT p.category) AS items FROM fact_sales f JOIN dim_product p USING (product_key)
            WHERE f.order_status NOT IN ('canceled','unavailable') GROUP BY {key}
        """).df()["items"].tolist()
        if only_multi:
            baskets = [b for b in baskets if len(b) > 1]
        multi = sum(len(b) > 1 for b in baskets)
        te = TransactionEncoder()
        X = pd.DataFrame(te.fit(baskets).transform(baskets, sparse=False), columns=te.columns_)
        min_sup = 20 / len(baskets)
        t0 = time.perf_counter(); fi_fp = fpgrowth(X, min_support=min_sup, use_colnames=True)
        t_fp = time.perf_counter() - t0
        t0 = time.perf_counter(); fi_ap = apriori(X, min_support=min_sup, use_colnames=True)
        t_ap = time.perf_counter() - t0
        pairs = fi_fp[fi_fp["itemsets"].apply(len) >= 2]
        lines += [f"**Giỏ = {level}**: {len(baskets):,} giỏ, chỉ {multi:,} giỏ ({100 * multi / len(baskets):.2f}%) "
                  f"có ≥ 2 danh mục. min_support = 20 giỏ ({min_sup:.5f}).",
                  f"- Tập phổ biến: FP-Growth {len(fi_fp)} ({t_fp:.2f}s), Apriori {len(fi_ap)} ({t_ap:.2f}s) — "
                  f"{'trùng khớp' if len(fi_fp) == len(fi_ap) else 'KHÁC NHAU'}; trong đó {len(pairs)} tập ≥ 2 mục.", ""]
        if len(pairs):
            # không đặt ngưỡng lift ở đây để thấy cả luật lift < 1; luật có ý nghĩa là lift > 1,2
            r = association_rules(fi_fp, metric="lift", min_threshold=0.0)
            r = r[r["antecedents"].apply(lambda s: min(s)) < r["consequents"].apply(lambda s: min(s))]
            n_sig = int((r["lift"] > 1.2).sum())
            lines += [f"- {len(r)} cặp luật (mỗi cặp A↔B tính một lần), {n_sig} cặp có lift > 1,2.", ""]
            r = r.sort_values("lift", ascending=False).head(8)
            r["luật"] = r["antecedents"].apply(lambda s: ", ".join(s)) + " → " + \
                        r["consequents"].apply(lambda s: ", ".join(s))
            r["giỏ"] = (r["support"] * len(baskets)).round().astype(int)
            lines += [_md(r[["luật", "giỏ", "support", "confidence", "lift"]], "{:.4f}"), ""]
    log.info("  luật kết hợp xong")
    return lines


def describe(con) -> list[str]:
    delay = con.sql("""
        SELECT CASE WHEN delay_days <= 0 THEN 'on time' WHEN delay_days <= 7 THEN 'late <= 7 days'
                    ELSE 'late > 7 days' END AS delivery,
               count(*) AS orders, round(avg(review_score), 2) AS avg_score,
               round(100 * avg((review_score <= 2)::INT), 1) AS pct_bad
        FROM fact_order WHERE order_status = 'delivered' AND review_score IS NOT NULL
        GROUP BY 1 ORDER BY 1""").df()
    basket = con.sql("""
        SELECT round(100 * avg((n_products = 1)::INT), 2) AS pct_orders_1_product
        FROM fact_order WHERE n_items IS NOT NULL""").df()
    repeat = con.sql("""
        SELECT round(100 * avg((n = 1)::INT), 2) AS pct_customers_1_order
        FROM (SELECT customer_key, count(*) n FROM fact_order GROUP BY 1)""").df()
    return ["## 0. Thống kê mô tả", "", _md(delay, "{:.2f}"), "",
            f"- Đơn chỉ có 1 sản phẩm: {basket.iloc[0, 0]}%",
            f"- Khách chỉ mua 1 đơn: {repeat.iloc[0, 0]}%", ""]


def run() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = _con()
    md = ["# Khai phá dữ liệu sơ bộ — Olist", ""]
    md += describe(con)
    md += classify_reviews(con)
    md += cluster_sellers(con)
    md += association(con)
    (OUT / "mining.md").write_text("\n".join(md), encoding="utf-8")
    log.info("Đã ghi %s", OUT / "mining.md")
