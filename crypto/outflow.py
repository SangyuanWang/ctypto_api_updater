#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: smangj
2025/8/25
mail: 631535207@qq.com
"""

from datetime import datetime

from crypto.info import ASSET_TOKEN, fetch_cryptoquant_data, ASSETS
from db_info import write_to_db

# === 1. 配置 ===
ASSET = "stablecoin"                 # 资产，例如 btc, eth
METRIC = "exchange-flows/outflow"  # 指标路径，参考文档

def token_in(window):
    for token in ASSET_TOKEN[ASSET]:
        params = {
            "window": window,
            "token": token,
            "exchange": "all_exchange",
            "limit": 1000,
        }
        df = fetch_cryptoquant_data(
            asset=ASSET,
            metric=METRIC,
            params=params,
        )
        df["token"] = token
        print(df.head())  # 看一下数据格式

        # 存数据库
        write_to_db(df, table_name=f"cryptoquant_outflow_{window}")
        print(f"✅ {token}数据写入完成！")


def asset_in(window):
    for asset in ASSETS:
        params = {
            "window": window,
            "exchange": "all_exchange",
            "limit": 100,
        }
        df = fetch_cryptoquant_data(
            asset=asset,
            metric=METRIC,
            params=params,
        )
        if df is None:
            continue
        else:
            df["token"] = asset
            print(df.head())  # 看一下数据格式

            # 存数据库
            write_to_db(df, table_name=f"cryptoquant_outflow_{window}")
            print(f"✅ {asset}数据写入完成！")


# === 4. 主流程 ===
if __name__ == "__main__":
    # 举例：拉 BTC Exchange Reserve，从 2023-01-01 到今天
    start_date = "2025-01-01"
    end_date = datetime.today().strftime("%Y-%m-%d")
    window = "day"
    asset_in(window)

