"""
示例主程序：读取 config.py 中配置的股票/ETF 代码并输出。

实际使用时，可以在此扩展数据抓取、计算估值分位和趋势信号的逻辑。
"""

from config import STOCK_CODES


def main() -> None:
    """打印配置中的股票代码列表。"""
    print("监控的股票/ETF 代码:")
    for code in STOCK_CODES:
        print(f"  - {code}")


if __name__ == "__main__":
    main()