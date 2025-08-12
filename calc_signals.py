# calc_signals.py
import os, sys, math
from datetime import datetime
import numpy as np
import pandas as pd
import tushare as ts

from config import STOCK_CODES, UNDERLYING_MAP

START_DATE = "20150101"  # 估值/均线计算的历史窗口；可加长
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
    vol   = pd.to_numeric(df["vol"], errors="coerce")  # 单位：手
    price = close.iloc[-1]
    ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else float("nan")
    vma20 = vol.rolling(20).mean().iloc[-1] if len(vol) >= 20 else float("nan")
    vnow  = vol.iloc[-1]
    return (price, ma200, vnow, vma20)

def stock_percentile(pro, ts_code: str):
    # 个股估值：优先 pe_ttm，缺失时用 pb
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
    # 指数估值：index_dailybasic 的 pe_ttm/pb
    df = pro.index_dailybasic(ts_code=index_code, start_date=START_DATE,
                              end_date=datetime.now().strftime("%Y%m%d"))
    df = df.sort_values("trade_date")
    if df.empty:
        return (float("nan"), "na", float("nan"))
    for m in ["pe_ttm", "pb"]:
        if m in df and pd.to_numeric(df[m], errors="coerce").notna().any():
            ser = pd.to_numeric(df[m], errors="coerce")
            latest = ser.dropna().iloc[-1]
            pct = pct_rank(ser, latest)
            return (latest, m, pct)
    return (float("nan"), "na", float("nan"))

def decide_action(percentile, trend, code):
    # 结合“估值分位 + 趋势 + 简化约束”生成动作建议
    # 规则：<30% → Add（若在200日线上方）；>70% → Trim；其余 Hold
    if pd.isna(percentile):
        return "Hold – no trade"
    if percentile > 70:
        return "Trim (≤5%)"
    if percentile < 30:
        if trend == "above":
            return "Add (≤5%)"
        else:
            return "Hold – wait for trend"  # 跌破200日线不建议加
    return "Hold – no trade"

def main():
    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        print("未检测到环境变量 TUSHARE_TOKEN。请在 GitHub Secrets 设置。")
        sys.exit(1)

    ts.set_token(token)
    pro = ts.pro_api()

    rows = []
    for code in STOCK_CODES:
        try:
            price, ma200, vnow, vma20 = get_ma_and_vol(pro, code)
            trend = "above" if (isinstance(ma200, float) and not math.isnan(ma200) and price >= ma200) else "below"
        except Exception as e:
            print(f"[{code}] 行情获取失败：{e}")
            price = ma200 = vnow = vma20 = float("nan")
            trend = "below"

        # 估值计算
        metric_name = "na"; latest_metric = float("nan"); percentile = float("nan")
        try:
            if code in UNDERLYING_MAP:     # 当作 ETF：用指数估值
                idx = UNDERLYING_MAP[code]
                latest_metric, metric_name, percentile = index_percentile(pro, idx)
            elif code.endswith(".SH") or code.endswith(".SZ"):
                latest_metric, metric_name, percentile = stock_percentile(pro, code)
        except Exception as e:
            print(f"[{code}] 估值获取失败：{e}")

        # 动作建议
        action = decide_action(percentile, trend, code)

        rows.append({
            "code": code,
            "metric_name": metric_name,
            "latest_metric": round(latest_metric, 4) if isinstance(latest_metric, (int,float)) and not math.isnan(latest_metric) else "",
            "percentile": percentile if not pd.isna(percentile) else "",
            "price": round(price, 4) if isinstance(price, (int,float)) and not math.isnan(price) else "",
            "ma200": round(ma200, 4) if isinstance(ma200, (int,float)) and not math.isnan(ma200) else "",
            "trend": trend,
            "vol_gt_vma20": (isinstance(vnow, float) and isinstance(vma20, float) and not math.isnan(vnow) and not math.isnan(vma20) and vnow > vma20),
            "action": action,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        })

    os.makedirs("data", exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT_PATH, index=False, encoding="utf-8")
    print(f"✅ 已生成 {OUT_PATH}")

if __name__ == "__main__":
    main()
