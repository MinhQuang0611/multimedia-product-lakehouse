from __future__ import annotations

import numpy as np
import pandas as pd

from src.common.config import get_logger, load_config, paths
from src.common.lake import scan, write_table

log = get_logger("silver.sim")

# u ∈ [0,1) lấy từ hash của giỏ hàng → các món cùng giỏ có xu hướng rơi vào cùng một shop.
def shop_pick(customer_id: str = "t.customer_id", t_dat: str = "t.t_dat", ps: str = "ps") -> str:
    u = f"((md5_number({customer_id} || '|' || {t_dat} || '|{load_config()['seed']}') % 1000000) / 1000000.0)"
    return f"{u} >= {ps}.cum_lo AND {u} < {ps}.cum_hi"


def _olist(con, t: str) -> str:
    return scan(paths().silver / "olist" / t)


def _write_df(con, df: pd.DataFrame, name: str) -> int:
    con.register("_df", df)
    n = write_table(con, "SELECT * FROM _df", paths().silver / "sim" / name,
                    manifest={"simulated": True, "seed": load_config()["seed"]})
    con.unregister("_df")
    log.info("  silver/sim/%-16s %10s dòng (mô phỏng)", name, f"{n:,}")
    return n


def olist_reference(con, n_shops: int) -> dict:
    shop_share = con.execute(f"""
        WITH s AS (SELECT seller_id, count(DISTINCT product_id) n FROM {_olist(con, 'order_item')} GROUP BY 1),
             b AS (SELECT n, ntile({n_shops}) OVER (ORDER BY n DESC, seller_id) AS bin FROM s)
        SELECT sum(n) / (SELECT sum(n) FROM s) FROM b GROUP BY bin ORDER BY bin
    """).fetchnumpy()
    score = con.execute(f"""
        SELECT review_score, count(*) / sum(count(*)) OVER () FROM {_olist(con, 'review')}
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    delay = con.execute(f"""
        SELECT date_diff('day', o.purchase_ts, r.review_creation_ts) AS d
        FROM {_olist(con, 'review')} r JOIN {_olist(con, 'order')} o USING (order_id)
        WHERE d BETWEEN 0 AND 120
    """).fetchnumpy()["d"]
    return {"shop_share": np.asarray(list(shop_share.values())[0], dtype=float),
            "score_p": np.array([p for _, p in score]), "delay_days": np.asarray(delay, dtype=int)}


def build_shops(con, ref: dict, sim: dict) -> pd.DataFrame:
    n = sim["n_shops"]
    w = ref["shop_share"] / ref["shop_share"].sum()
    shops = pd.DataFrame({
        "shop_id": [f"S{i + 1}" for i in range(n)],
        "shop_name": [f"Shop {chr(ord('A') + i)}" for i in range(n)],
        "size_rank": np.arange(1, n + 1),
        "weight": w,
        "node_id": sim["node_of_shop"][:n],
    })
    _write_df(con, shops, "shop")
    return shops


def build_product_shop(con, shops: pd.DataFrame, sim: dict, rng: np.random.Generator) -> pd.DataFrame:
    arts = con.execute(f"SELECT article_id FROM {scan(paths().silver / 'hm' / 'article')} ORDER BY 1").df()
    p_k = np.asarray(sim["shops_per_product"], dtype=float)
    k = rng.choice(np.arange(1, len(p_k) + 1), size=len(arts), p=p_k / p_k.sum())
    w = shops["weight"].to_numpy()

    # Rút k shop không hoàn lại ∝ trọng số: Gumbel-top-k (tương đương, vector hoá được).
    keys = np.log(w)[None, :] - np.log(-np.log(rng.random((len(arts), len(w)))))
    order = np.argsort(-keys, axis=1)
    rows_a, rows_s = [], []
    for kk in range(1, len(p_k) + 1):
        idx = np.where(k == kk)[0]
        chosen = np.sort(order[idx, :kk], axis=1)          # thứ tự shop cố định → đoạn [0,1) ổn định
        rows_a.append(np.repeat(idx, kk))
        rows_s.append(chosen.ravel())
    a_idx, s_idx = np.concatenate(rows_a), np.concatenate(rows_s)
    ps = pd.DataFrame({"article_id": arts["article_id"].to_numpy()[a_idx],
                       "shop_id": shops["shop_id"].to_numpy()[s_idx], "w": w[s_idx]})
    ps = ps.sort_values(["article_id", "shop_id"], ignore_index=True)
    tot = ps.groupby("article_id")["w"].transform("sum")
    ps["cum_hi"] = ps.groupby("article_id")["w"].cumsum() / tot
    ps["cum_lo"] = ps["cum_hi"] - ps["w"] / tot
    ps.loc[~ps["article_id"].duplicated(keep="last"), "cum_hi"] = 1.0   # tránh sai số làm tròn ở đầu mút
    ps = ps.drop(columns="w")
    _write_df(con, ps, "product_shop")
    return ps


def build_reviews(con, ref: dict, sim: dict, rng: np.random.Generator) -> pd.DataFrame:
    con.execute(f"CREATE OR REPLACE TEMP VIEW _ps AS SELECT * FROM {scan(paths().silver / 'sim' / 'product_shop')}")
    lines = con.execute(f"""
        SELECT t.t_dat, t.customer_id, t.article_id, ps.shop_id, t.sales_qty
        FROM {scan(paths().silver / 'hm' / 'transaction')} t
        JOIN _ps ps ON ps.article_id = t.article_id AND {shop_pick()}
        ORDER BY t.t_dat, t.customer_id, t.article_id, ps.shop_id
    """).df()

    units = lines.groupby("article_id")["sales_qty"].sum()
    pop = np.log1p(units)
    pop = (pop - pop.mean()) / pop.std()
    quality = pd.Series(rng.normal(0, sim["quality_sd"], len(units)) + sim["popularity_effect"] * pop.to_numpy(),
                        index=units.index)

    rev = lines[rng.random(len(lines)) < sim["review_rate"]].reset_index(drop=True)
    z = quality.loc[rev["article_id"]].to_numpy() + rng.logistic(0, 1, len(rev))
    # Hiệu chỉnh: ngưỡng cắt = phân vị của z tại tỷ lệ tích luỹ của Olist → phân phối điểm khớp D3.
    cuts = np.quantile(z, np.cumsum(ref["score_p"])[:-1])
    rev["score"] = (1 + np.searchsorted(cuts, z, side="right")).astype("int8")
    rev["review_date"] = rev["t_dat"] + pd.to_timedelta(rng.choice(ref["delay_days"], len(rev)), unit="D")
    rev.insert(0, "review_id", [f"R{i:08d}" for i in range(len(rev))])
    rev = rev.rename(columns={"t_dat": "purchase_date"}).drop(columns="sales_qty")
    _write_df(con, rev, "review")
    return rev


def build_product_changes(con, sim: dict, rng: np.random.Generator) -> pd.DataFrame:
    a = scan(paths().silver / "hm" / "article")
    sold = con.execute(f"""
        SELECT ar.article_id, ar.department_no, ar.section_no, ar.index_group_no,
               min(t.t_dat) AS lo, max(t.t_dat) AS hi
        FROM {a} ar JOIN {scan(paths().silver / 'hm' / 'transaction')} t USING (article_id)
        GROUP BY ALL HAVING date_diff('day', lo, hi) >= 30 ORDER BY article_id
    """).df()
    depts = con.execute(f"SELECT DISTINCT department_no, section_no, index_group_no FROM {a}").df()

    pick = sold[rng.random(len(sold)) < sim["scd2_change_rate"]].reset_index(drop=True)
    old = []
    for r in pick.itertuples():
        # Department cũ: cùng section nếu có, không thì cùng index_group (một lần sắp xếp lại gian hàng).
        cand = depts[(depts.section_no == r.section_no) & (depts.department_no != r.department_no)]
        if cand.empty:
            cand = depts[(depts.index_group_no == r.index_group_no) & (depts.department_no != r.department_no)]
        old.append(int(rng.choice(cand["department_no"].to_numpy())) if len(cand) else None)
    pick["old_department_no"] = old
    span = (pick["hi"] - pick["lo"]).dt.days.to_numpy()
    pick["change_date"] = pick["lo"] + pd.to_timedelta((rng.random(len(pick)) * (span - 1)).astype(int) + 1, unit="D")
    out = pick.dropna(subset=["old_department_no"])[
        ["article_id", "change_date", "old_department_no", "department_no"]].rename(
        columns={"department_no": "new_department_no"})
    out["old_department_no"] = out["old_department_no"].astype(int)
    _write_df(con, out.reset_index(drop=True), "product_change")
    return out


def write_report(ref: dict, shops: pd.DataFrame, ps: pd.DataFrame, rev: pd.DataFrame, chg: pd.DataFrame,
                 sim: dict) -> None:
    k = ps.groupby("article_id").size().value_counts(normalize=True).sort_index()
    per_shop = ps.groupby("shop_id").size()
    sc = rev["score"].value_counts(normalize=True).sort_index()
    by_art = rev.groupby("article_id")["score"].agg(["mean", "size"])
    corr = np.corrcoef(np.log1p(by_art["size"]), by_art["mean"])[0, 1]
    md = ["# Dữ liệu mô phỏng cho H&M (silver/sim)", "",
          "Toàn bộ bảng trong `silver/sim/` là **dữ liệu sinh** (seed cố định), không phải dữ liệu H&M thật.", "",
          "## Shop — trọng số từ phân phối số sản phẩm/seller của Olist", "",
          "| shop | node | trọng số (Olist) | số article được gán |", "|---|---|---|---|",
          *[f"| {r.shop_id} {r.shop_name} | {r.node_id} | {r.weight:.3f} | {per_shop.get(r.shop_id, 0):,} |"
            for r in shops.itertuples()],
          "", "Số shop/article: " + ", ".join(f"{i} shop {p:.1%}" for i, p in k.items())
          + f" — trung bình {ps.groupby('article_id').size().mean():.2f}", "",
          "## Review — phân phối điểm sau hiệu chỉnh so với Olist", "",
          f"- {len(rev):,} review trên {rev['article_id'].nunique():,} article (review_rate = {sim['review_rate']}).",
          f"- Tương quan log(số review) ~ điểm TB theo article: {corr:.3f} (hiệu ứng độ phổ biến).", "",
          "| điểm | Olist | mô phỏng |", "|---|---|---|",
          *[f"| {s} | {ref['score_p'][s - 1]:.3f} | {sc.get(s, 0):.3f} |" for s in range(1, 6)],
          "", f"Độ trễ mua → review rút từ Olist: trung vị {int(np.median(ref['delay_days']))} ngày.", "",
          "## Lịch sử đổi department (SCD Type 2)", "",
          f"- {len(chg):,} article có một lần đổi department; giá trị hiện tại là snapshot thật của H&M.", ""]
    out = paths().reports / "hm"
    out.mkdir(parents=True, exist_ok=True)
    (out / "simulation.md").write_text("\n".join(md), encoding="utf-8")


def run(con) -> None:
    cfg = load_config()
    sim, rng = cfg["simulation"], np.random.default_rng(cfg["seed"])
    log.info("Silver mô phỏng (H&M + tham chiếu Olist)")
    ref = olist_reference(con, sim["n_shops"])
    shops = build_shops(con, ref, sim)
    ps = build_product_shop(con, shops, sim, rng)
    rev = build_reviews(con, ref, sim, rng)
    chg = build_product_changes(con, sim, rng)
    write_report(ref, shops, ps, rev, chg, sim)
