#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import timedelta, datetime

import pandas as pd
import requests
import time

from CryptoQuantAPI import write_to_db

"""
author: smangj
2025/8/26
mail: 631535207@qq.com
"""


def fetch_asset_metrics_safe(assets, metrics, start_time, end_time, frequency="1d", page_size=100, sleep=0.7):
    """
    社区版 CoinMetrics API 安全请求 (分页 + 限速)
    """
    for asset in assets:
        base_url = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
        params = {
            "assets": asset,
            "metrics": metrics,
            "frequency": frequency,
            "start_time": start_time,
            "end_time": end_time,
            "page_size": page_size
        }

        all_data = []
        url = base_url
        while url:
            r = requests.get(url, params=params if url == base_url else None)
            if r.status_code == 429:
                print("Rate limit hit, sleeping...")
                time.sleep(6)  # 等一整个窗口
                continue
            try:
                r.raise_for_status()
            except Exception as e:
                print(e)
                break
            result = r.json()
            all_data.extend(result.get("data", []))
            url = result.get("next_page_url")
            params = None
            time.sleep(sleep)  # 限速
            print("sleep")

    return pd.DataFrame(all_data)

def assets():

    url = "https://community-api.coinmetrics.io/v4/catalog/assets"
    r = requests.get(url)
    r.raise_for_status()

    assets = r.json()["data"]
    return pd.DataFrame(assets)


if __name__ == "__main__":
    metric = "AdrActContCnt"
    # # assets = ["btc", "bsv", "bch", "btg", "dash", "doge", "etc", "eth", "ltc", "vtc", "xmr", "zec"]
    # assets = ["dcr", "dgb", "grin", "xvg"]
    all = pd.read_csv("all_data.csv")
    all_assets = all["symbol"].tolist()
    b = assets()
    cm_assets = b["asset"].tolist()

    assets = list(set(all_assets) & set(cm_assets))
    start_date = "2023-01-01"
    end_date = datetime.today().strftime("%Y-%m-%d")
    response = fetch_asset_metrics_safe(assets, metric, start_date, end_date)
    if not response.empty:
        write_to_db(response, f"CoinMetrics_{metric}")


