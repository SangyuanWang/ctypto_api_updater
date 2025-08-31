#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: smangj
2025/8/25
mail: 631535207@qq.com
"""

from datetime import datetime
from crypto.info import fetch_cryptoquant_data, ASSETS, ASSET_TOKEN, EXCHANGE
from db_info import write_to_db

# === 1. 配置 ===
ASSET = "stablecoin"                 # 资产，例如 btc, eth
METRIC = "exchange-flows/inflow"  # 指标路径，参考文档

def token_in(window, exchange):
    for token in ASSET_TOKEN[ASSET]:
        params = {
            "window": window,
            "token": token,
            "exchange": exchange,
            "limit": 1000,
        }
        df = fetch_cryptoquant_data(
            asset=ASSET,
            metric=METRIC,
            params=params,
        )
        if df is None:
            continue
        df["asset"] = ASSET
        df["token"] = token
        df["exchange"] = exchange
        print(df.head())  # 看一下数据格式

        # 存数据库
        write_to_db(df, table_name=f"cryptoquant_inflow_{window}")
        print(f"✅ {token}数据写入完成！")


def asset_in(window, exchange):
    for asset in ASSETS:
        params = {
            "window": window,
            "exchange": exchange,
            "limit": 1000,
        }
        df = fetch_cryptoquant_data(
            asset=asset,
            metric=METRIC,
            params=params,
        )
        if df is None:
            continue
        else:
            df["asset"] = asset
            df["exchange"] = exchange
            print(df.head())  # 看一下数据格式

            # 存数据库
            write_to_db(df, table_name=f"cryptoquant_inflow_{window}")
            print(f"✅ {asset}数据写入完成！")


# === 4. 主流程 ===
if __name__ == "__main__":

    window = "day"
    for ex in EXCHANGE:
        token_in(window, ex)
        asset_in(window, ex)



