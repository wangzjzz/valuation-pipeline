# calc_signals.py
import os, sys, math, time
import numpy as np
import pandas as pd
import tushare as ts
from datetime import datetime, timedelta
import zoneinfo

from config import STOCK_CODES, UNDERLYING_MAP

START_DATE = "20150101"
OUT_PATH = "data/today_signals.csv"
CN_TZ = zoneinfo.ZoneInfo("Asia/Shanghai")

def pct_rank(series: pd.Series, value: float) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0 or pd.isna(value):
        return float("nan")
    return round((s.lt(value).sum() / len(s)) * 100.0, 1)

def cn_end_date():
    now_cn = datetime.now(tz=CN_TZ)
    use_date = now_cn - timedelta(days=1) if now_cn.hour < 17 else now_cn
    return use_date.strftime("%Y%m%d")

def fetch_with_backoff(fetch_fn, max_back=3, sleep=0.5):
    base = datetime.strptime(cn_end_date(), "%Y%m%d")
    last_err = None
    for d in range(max_back + 1):
        ed = (base - timedelta(days=d)).strftime("%Y%m%d")
        try:
            df = fetch_fn(ed)
            if df is not None and not df.empty:
                return df, ed
        except Exception as e:
            last_err = e
            time.sleep(sleep)
    if last_err:
        raise last_err
    return None, base.strftime("%Y%m%d")

def get_ma_and_vol(pro, ts_code: str):
    """价格/MA：stock日线为空→ETF改用fund_daily；不足200日用MA120。"""
    def _pull_stock(ed):
        return pro.daily(ts_code=ts_code, start_date=START_DATE, end_date=ed)
    df, used_end = fetch_with_backoff(_pull_stock, max_back=3)
    note = f"end={used_end}"

    if df is None or df.empty:
        # ETF 更稳的取法：基金日线
        def _pull_fund(ed):
            return pro.fund_daily(ts_code=ts_code, start_date=START_DATE, end_date=ed)
        df, used_end2 = fetch_with_backoff(_pull_fund, max_back=3)
        if df is not None and not df.empty:
            note = f"fund_end={used_end2}"
        else:
            return (float('nan'), float('nan'), float('nan'), float('nan'), note + ";no_daily_data")

    df = df.sort_values("trade_date")
    close = pd.to_numeric(df["close"], errors="coerce")
    # fund_daily 可能没有 vol；退而求其次用 amount
    vol = pd.to_numeric(df["vol"], errors="coerce") if "vol" in df.columns else pd.to_numeric(df.get("amount"), errors="coerce")
    price = close.dropna().iloc[-1] if close.dropna().size else float("nan")

    ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else float("nan")
    if (isinstance(ma200, float) and math.isnan(ma200)) and len(close) >= 120:
        ma200 = close.rolling(120).mean().iloc[-1]
        note += ";use_ma120"

    vma20 = vol.rolling(20).mean().iloc[-1] if vol is not None and len(vol) >= 20 else float("nan")
    vnow  = vol.iloc[-1] if vol is not None and len(vol) else float("nan")
    return (price, ma200, vnow, vma20, note)

def stock_percentile(pro, ts_code: str):
    def _pull(ed):
        return pro.daily_basic(ts_code=ts_code, start_date=START_DATE, end_date=ed,
                               fields="trade_date,pe_ttm,pb")
    df, used_end = fetch_with_backoff(_pull, max_back=3)
    if df is None or df.empty:
        return (float("nan"), "na", float("nan"), f"end={used_end};no_daily_basic")
    df = df.sort_values("trade_date")
    pe = pd.to_numeric(df["pe_ttm"], errors="coerce")
    pb = pd.to_numeric(df["pb"], errors="coerce")
    if pe.notna().any():
        latest = pe.dropna().iloc[-1]
        return (latest, "pe_ttm", pct_rank(pe, latest), f"end={used_end}")
    if pb.notna().any():
        latest = pb.dropna().iloc[-1]
        return (latest, "pb", pct_rank(pb, latest), f"end={used_end}")
    return (float("nan"), "na", float("nan"), f"end={used_end};no_pe_pb")

def index_percentile(pro, index_code: str):
    """指数估值：优先 tushare index_dailybasic；为空则用 akshare/csindex 兜底。"""
    def _pull(ed):
        return pro.index_dailybasic(ts_code=index_code, start_date=START_DATE, end_date=ed)
    df, used_end = fetch_with_backoff(_pull, max_back=3)
    if df is not None and not df.empty:
        df = df.sort_values("trade_date")
        for m in ["pe_ttm", "pb"]:
            ser = pd.to_numeric(df[m], errors="coerce")
            if ser.notna().any():
                latest = ser.dropna().iloc[-1]
                return (latest, m, pct_rank(ser, latest), f"end={used_end}")
    # —— tushare 为空：用 akshare 的中证指数估值兜底 ——
    try:
        import akshare as ak
        sym = index_code.split('.')[0]  # 去掉 .SH/.SZ/.CSI
        val_df = ak.stock_zh_index_value_csindex(symbol=sym)
        if val_df is not None and not val_df.empty:
            # 兼容不同版本列名（PE 或 pe/pe_ttm；PB 或 pb）
            cols = {c.lower(): c for c in val_df.columns}
            # 优先PE，其次PB
            for col_key, mname in [("pe", "pe_ttm"), ("pb", "pb")]:
                if col_key in cols:
                    ser = pd.to_numeric(val_df[cols[col_key]], errors="coerce").dropna()
                    if not ser.empty:
                        latest = float(ser.iloc[-1])
                        return (latest, mname, pct_rank(ser, latest), f"akshare_csindex:{sym}")
    except Exception as e:
        return (float("nan"), "na", float("nan"), f"no_index_basic:{index_code};ak_err:{e}")

    return (float("nan"), "na", float("nan"), f"no_index_basic:{index_code};ak_empty")

def decide_action(percentile, trend):
    # 主观模式里你也可以直接改成别的；这里保留一个保守版兜底
    if pd.isna(percentile):
        return "Hold – no trade"
    if percentile > 70:
        return "Trim (≤5%)"
    if percentile < 30:
        return "Add (≤5%)" if trend == "above" else "Hold – wait for trend"
    return "Hold – no trade"

def main():
    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        print("未检测到环境变量 TUSHARE_TOKEN（请在 Secrets 设置）。")
        sys.exit(1)
    ts.set_token(token)
    pro = ts.pro_api()

    rows = []
    for code in STOCK_CODES:
        # 价格/趋势
        try:
            price, ma200, vnow, vma20, note1 = get_ma_and_vol(pro, code)
            trend = "above" if (isinstance(ma200,float) and not math.isnan(ma200)
                                 and isinstance(price,float) and not math.isnan(price)
                                 and price >= ma200) else "below"
        except Exception as e:
            price = ma200 = vnow = vma20 = float("nan")
            trend = "below"
            note1 = f"ma_err:{e}"

        # 估值（ETF→指数；其他→个股）
        metric_name = "na"; latest_metric = float("nan"); percentile = float("nan"); note2 = ""
        try:
            if code in UNDERLYING_MAP and UNDERLYING_MAP[code]:
                idx = UNDERLYING_MAP[code]
                latest_metric, metric_name, percentile, note2 = index_percentile(pro, idx)
            else:
                latest_metric, metric_name, percentile, note2 = stock_percentile(pro, code)
        except Exception as e:
            note2 = f"val_err:{e}"

        note = ";".join([x for x in [note1, note2] if x])

        rows.append({
            "code": code,
            "metric_name": metric_name,
            "latest_metric": round(latest_metric,4) if isinstance(latest_metric,(int,float)) and not math.isnan(latest_metric) else "",
            "percentile": percentile if not pd.isna(percentile) else "",
            "price": round(price,4) if isinstance(price,(int,float)) and not math.isnan(price) else "",
            "ma200": round(ma200,4) if isinstance(ma200,(int,float)) and not math.isnan(ma200) else "",
            "trend": trend,
            "vol_gt_vma20": (isinstance(vnow,float) and isinstance(vma20,float)
                             and not math.isnan(vnow) and not math.isnan(vma20) and vnow>vma20),
            "action": decide_action(percentile, trend),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "note": note,
        })

    os.makedirs("data", exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT_PATH, index=False, encoding="utf-8")
    print(f"✅ 已生成 {OUT_PATH}")

if __name__ == "__main__":
    main()
