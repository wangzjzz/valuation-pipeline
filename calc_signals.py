# calc_signals.py
import os, sys, math, time
import numpy as np
import pandas as pd
import tushare as ts
from datetime import datetime, timedelta
import zoneinfo

# ===== 你的自定义配置（放在仓库根目录的 config.py）=====
from config import STOCK_CODES, UNDERLYING_MAP

# 历史窗口与输出路径
START_DATE = "20150101"
OUT_PATH = "data/today_signals.csv"
CN_TZ = zoneinfo.ZoneInfo("Asia/Shanghai")

def pct_rank(series: pd.Series, value: float) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0 or pd.isna(value):
        return float("nan")
    return round((s.lt(value).sum() / len(s)) * 100.0, 1)

def cn_end_date():
    """若上海时间 <17:00，用昨天；否则用今天。"""
    now_cn = datetime.now(tz=CN_TZ)
    use_date = now_cn - timedelta(days=1) if now_cn.hour < 17 else now_cn
    return use_date.strftime("%Y%m%d")

def fetch_with_backoff(fetch_fn, max_back=3, sleep=0.5):
    """按 end_date 向前回退最多 max_back 天，直到拿到非空数据。"""
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
    """行情与均线；不足200日用MA120兜底。"""
    def _pull(ed):
        return pro.daily(ts_code=ts_code, start_date=START_DATE, end_date=ed)
    df, used_end = fetch_with_backoff(_pull, max_back=3)
    note = f"end={used_end}"
    if df is None or df.empty:
        return (float("nan"), float("nan"), float("nan"), float("nan"), note + ";no_daily_data")
    df = df.sort_values("trade_date")
    close = pd.to_numeric(df["close"], errors="coerce")
    vol   = pd.to_numeric(df["vol"], errors="coerce")
    price = close.dropna().iloc[-1] if close.dropna().size else float("nan")
    ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else float("nan")
    if (isinstance(ma200, float) and math.isnan(ma200)) and len(close) >= 120:
        ma200 = close.rolling(120).mean().iloc[-1]
        note += ";use_ma120"
    vma20 = vol.rolling(20).mean().iloc[-1] if len(vol) >= 20 else float("nan")
    vnow  = vol.iloc[-1] if len(vol) else float("nan")
    return (price, ma200, vnow, vma20, note)

def stock_percentile(pro, ts_code: str):
    """个股估值：优先 pe_ttm，缺失则 pb。"""
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
    """指数估值：index_dailybasic 的 pe_ttm / pb。"""
    def _pull(ed):
        return pro.index_dailybasic(ts_code=index_code, start_date=START_DATE, end_date=ed)
    df, used_end = fetch_with_backoff(_pull, max_back=3)
    if df is None or df.empty:
        return (float("nan"), "na", float("nan"), f"end={used_end};no_index_basic:{index_code}")
    df = df.sort_values("trade_date")
    for m in ["pe_ttm", "pb"]:
        ser = pd.to_numeric(df[m], errors="coerce")
        if ser.notna().any():
            latest = ser.dropna().iloc[-1]
            return (latest, m, pct_rank(ser, latest), f"end={used_end}")
    return (float("nan"), "na", float("nan"), f"end={used_end};index_no_pe_pb:{index_code}")

def decide_action(percentile, trend):
    """根据你的规则产出动作：<30%加（需在200日线上），>70%减，其余Hold。"""
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
        # 行情与趋势
        try:
            price, ma200, vnow, vma20, note1 = get_ma_and_vol(pro, code)
            trend = "above" if (isinstance(ma200,float) and not math.isnan(ma200) and isinstance(price,float) and not math.isnan(price) and price>=ma200) else "below"
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
                # 未映射也尝试按个股处理（ETF多半拿不到估值，这是正常的）
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
            "vol_gt_vma20": (isinstance(vnow,float) and isinstance(vma20,float) and not math.isnan(vnow) and not math.isnan(vma20) and vnow>vma20),
            "action": decide_action(percentile, trend),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "note": note,
        })

    os.makedirs("data", exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT_PATH, index=False, encoding="utf-8")
    print(f"✅ 已生成 {OUT_PATH}")

if __name__ == "__main__":
    main()
