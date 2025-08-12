# calc_signals.py
import os, sys, math
from datetime import datetime
import numpy as np
import pandas as pd
import tushare as ts

# ===== 你的配置（请先确保 config.py 已在仓库根目录）=====
from config import STOCK_CODES, UNDERLYING_MAP

START_DATE = "20150101"
OUT_PATH = "data/today_signals.csv"

def pct_rank(series: pd.Series, value: float) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0 or pd.isna(value):
        return float("nan")
    return round((s.lt(value).sum() / len(s)) * 100.0, 1)

def get_ma_and_vol(pro, ts_code: str):
    df = pro.daily(ts_code=ts_code, start_date=START_DATE,
                   end_date=datetime.now().strftime("%Y%m%d"))
    df = df.sort_values("trade_date")
    if df.empty:
        return (float("nan"), float("nan"), float("nan"), float("nan"))
    close = pd.to_numeric(df["close"], errors="coerce")
    vol   = pd.to_numeric(df["vol"], errors="coerce")
    price = close.iloc[-1]
    ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else float("nan")
    vma20 = vol.rolling(20).mean().iloc[-1] if len(vol) >= 20 else float("nan")
    vnow  = vol.iloc[-1]
    return (price, ma200, vnow, vma20)

def stock_percentile(pro, ts_code: str):
    df = pro.daily_basic(ts_code=ts_code, start_date=START_DATE,
                         end_date=datetime.now().strftime("%Y%m%d"),
                         fields="trade_date,pe_ttm,pb")
    df = df.sort_values("trade_date")
    if df.empty:
        return (float("nan"), "na", float("nan"))
    pe = pd.to_numeric(df["pe_ttm"], errors="coerce")
    pb = pd.to_numeric(df["pb"], errors="coerce")
    if pe.notna().any():
        latest = pe.dropna().iloc[-1]
        pct = pct_rank(pe, latest)
        return (latest, "pe_ttm", pct)
    elif pb.notna().any():
        latest = pb.dropna().iloc[-1]
        pct = pct_rank(pb, latest)
        return (latest, "pb", pct)
    return (float("nan"), "na", float("nan"))

def index_percentile(pro, index_code: str):
    df = pro.index_dailybasic(ts_code=index_code, start_date=START_DATE,
                              end_date=datetime.now().strftime("%Y%m%d"))
    df = df.sort_values("trade_date")
    if df.empty:
        return (float("nan"), "na", float("nan"))
    for m in ["pe_ttm", "pb"]:
        ser = pd.to_numeric(df[m], errors="coerce")
        if ser.notna().any():
            latest = ser.dropna().iloc[-1]
            pct = pct_rank(ser, latest)
            return (latest, m, pct)
    return (float("nan"), "na", float("nan"))

def decide_action(percentile, trend):
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
        print("未检测到环境变量 TUSHARE_TOKEN（请到仓库 Settings → Secrets 添加）。")
        sys.exit(1)

    ts.set_token(token)
    pro = ts.pro_api()

    rows = []
    for code in STOCK_CODES:
        # 价格&趋势
        try:
            price, ma200, vnow, vma20 = get_ma_and_vol(pro, code)
            trend = "above" if (isinstance(ma200,float) and not math.isnan(ma200) and price>=ma200) else "below"
        except Exception as e:
            print(f"[{code}] 行情失败：{e}")
            price = ma200 = float("nan"); trend = "below"; vnow = vma20 = float("nan")

        # 估值（个股直接取，ETF用指数估值）
        metric_name = "na"; latest_metric = float("nan"); percentile = float("nan")
        try:
            if code in UNDERLYING_MAP:
                idx = UNDERLYING_MAP[code]
                latest_metric, metric_name, percentile = index_percentile(pro, idx)
            else:
                latest_metric, metric_name, percentile = stock_percentile(pro, code)
        except Exception as e:
            print(f"[{code}] 估值失败：{e}")

        action = decide_action(percentile, trend)

        rows.append({
            "code": code,
            "metric_name": metric_name,
            "latest_metric": round(latest_metric,4) if isinstance(latest_metric,(int,float)) and not math.isnan(latest_metric) else "",
            "percentile": percentile if not pd.isna(percentile) else "",
            "price": round(price,4) if isinstance(price,(int,float)) and not math.isnan(price) else "",
            "ma200": round(ma200,4) if isinstance(ma200,(int,float)) and not math.isnan(ma200) else "",
            "trend": trend,
            "vol_gt_vma20": (isinstance(vnow,float) and isinstance(vma20,float) and not math.isnan(vnow) and not math.isnan(vma20) and vnow>vma20),
            "action": action,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        })

    os.makedirs("data", exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT_PATH, index=False, encoding="utf-8")
    print(f"✅ 已生成 {OUT_PATH}")

if __name__ == "__main__":
    main()
