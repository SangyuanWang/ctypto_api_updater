#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: smangj
2025/8/25
mail: 631535207@qq.com
"""

from datetime import datetime

import pandas as pd
import requests

from crypto.info import API_KEY
from db_info import write_to_db

# === 1. 配置 ===
ASSET = "stablecoin"                 # 资产，例如 btc, eth
METRIC = "exchange-flows/inflow"  # 指标路径，参考文档
tokens = ["tusd", "gusd", "usdp", "susd", "dai", "usdk", "usdt_eth", "husd", "all_token", "usdc", "sai", "busd"]

def fetch_cryptoquant_data(asset, metric, token, start, end, window="day", limit=366):
    url = f"https://api.cryptoquant.com/v1/{asset}/{metric}"
    headers = {"Authorization": f"Bearer {API_KEY}"}

    params = {
        "window": window,
        "token": token,
        # "from": start,
        # "to": end,
        "exchange": "all_exchange",
        "limit": limit,
    }
    resp = requests.get(url, headers=headers, params=params)
    # resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json().get("result", {})
    data = data.get("data", [])
    return pd.DataFrame(data)

# === 4. 主流程 ===
if __name__ == "__main__":
    # 举例：拉 BTC Exchange Reserve，从 2023-01-01 到今天
    start_date = "2025-01-01"
    end_date = datetime.today().strftime("%Y-%m-%d")
    for token in tokens:
        window = "day"
        df = fetch_cryptoquant_data(
            asset=ASSET,
            metric=METRIC,
            token=token,
            start=start_date,
            end=end_date,
            window=window,
            limit=1000
        )
        df["token"] = token
        print(df.head())  # 看一下数据格式

        # 存数据库
        write_to_db(df, table_name=f"cryptoquant_inflow_{window}")
        print(f"✅ {token}数据写入完成！")

