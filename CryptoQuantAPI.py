#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: smangj
2025/8/25
mail: 631535207@qq.com
"""

import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# === 1. 配置 ===
API_KEY = "qYh3Ex0xTVt4I1nmQmDap2iQCZxunclIT7TL1mcCnlTExjV8Q2UpTKVkoqmpLERTLOLgaMFhOhcgb9530JCh9oPG"
ASSET = "btc"                 # 资产，例如 btc, eth
METRIC = "network-data/difficulty"  # 指标路径，参考文档
DB_URL = "postgresql://postgres:Vk3rUTQjcweSkOGO@abel-sit.cwbe6kuqcn4q.us-east-1.rds.amazonaws.com:5432/abel-test"  # 你等下提供

# === 2. 拉数据函数 ===
def fetch_cryptoquant_data(asset, metric, start, end, window="day", limit=366):
    url = f"https://api.cryptoquant.com/v1/{asset}/{metric}"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    params = {
        "window": window,
        # "from": start,
        # "to": end,
        "limit": limit
    }
    resp = requests.get(url, headers=headers, params=params)
    # resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json().get("result", [])
    data = data.get("data", [])
    return pd.DataFrame(data)

# === 3. 入库函数 ===
def write_to_db(df, table_name, db_url=DB_URL):
    engine = create_engine(db_url)
    df.to_sql(table_name, engine, if_exists="replace", index=False)

# === 4. 主流程 ===
if __name__ == "__main__":
    # 举例：拉 BTC Exchange Reserve，从 2024-01-01 到今天
    start_date = "2024-09-01"
    end_date = datetime.today().strftime("%Y-%m-%d")

    window = "day"
    df = fetch_cryptoquant_data(
        asset=ASSET,
        metric=METRIC,
        start=start_date,
        end=end_date,
        window=window
    )

    print(df.head())  # 看一下数据格式

    # 存数据库
    write_to_db(df, table_name=f"cryptoquant_{ASSET}_'difficulty'_{window}")
    print("✅ 数据写入完成！")

