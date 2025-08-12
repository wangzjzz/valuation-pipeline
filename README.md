# stock_signal 项目概述

这个文件夹包含了一个示例工程的基本框架，用于每日生成股票/ETF 的估值分位和趋势信号。

## 目录结构

- `config.py`：配置文件，列出需要监控的股票和 ETF 代码等设置。
- `main.py`：示例主程序，读取配置并打印股票代码。后续可在此扩展逻辑。
- `data/`：用于存放每日生成的信号数据，例如 `today_signals.csv`。

## 使用说明

1. 安装依赖：
   ```bash
   python3 -m pip install tushare pandas akshare
   ```

2. 在 [TuShare 官网](https://tushare.pro) 申请并升级到所需积分档位（建议 ≥ 2000 分），获取 token。

3. 将 token 添加到你的环境变量或者代码中，后续的主程序会使用它访问 TuShare API。

4. 修改 `config.py` 中的 `STOCK_CODES` 列表，确保列出你需要监控的所有代码（使用 ts_code 格式）。

5. 在 `main.py` 中实现逻辑：读取代码列表，调用数据接口获取估值和均线信息，生成 `today_signals.csv` 保存到 `data/` 目录。

6. 通过定时任务（如 cron 或 GitHub Actions）每天执行你的脚本，自动更新信号。

## 进一步扩展

如果暂时没有足够的 TuShare 积分，可以考虑：

* 使用 [AkShare](https://akshare.xyz) 提供的部分免费接口获取指数估值和行情数据作为替代。
* 参考基金公司官网或中证指数官网，手动拉取指数估值数据。

该项目只是一个起点，你可以根据自己的需求进一步完善。
