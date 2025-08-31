#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: smangj
2025/8/31
mail: 631535207@qq.com
"""
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy import inspect
import pandas as pd
from crypto.info import fetch_cryptoquant_data, METRICS_EXCHANGE_MAP, EXCHANGE, ASSET_TOKEN, ASSETS, METRICS_MAP
from db_info import DB_URL
import logging
logging.basicConfig(level=logging.INFO)

STABELCOIN_CONFIGS = [
    {
        "table_name": f"cryptoquant_{metric_name}_day",
        "asset": "stablecoin",
        "metric": metric_url,
        "params": {"window": "day", "exchange": exchange, "token": token}
    }
    for metric_name, metric_url in METRICS_EXCHANGE_MAP.items()
    for exchange in EXCHANGE
    for token in ASSET_TOKEN["stablecoin"]] + [
    {
        "table_name": f"cryptoquant_{metric_name}_day",
        "asset": "stablecoin",
        "metric": metric_url,
        "params": {"window": "day", "token": token}
    }
    for metric_name, metric_url in METRICS_MAP.items()
    for token in ASSET_TOKEN["stablecoin"]]

ASSETS_CONFIGS = [{
        "table_name": f"cryptoquant_{metric_name}_day",
        "asset": asset,
        "metric": metric_url,
        "params": {"window": "day", "exchange": exchange}
    }
    for metric_name, metric_url in METRICS_EXCHANGE_MAP.items()
    for exchange in EXCHANGE
    for asset in ASSETS] + [{
        "table_name": f"cryptoquant_{metric_name}_day",
        "asset": asset,
        "metric": metric_url,
        "params": {"window": "day"}
    }
    for metric_name, metric_url in METRICS_MAP.items()
    for asset in ASSETS]

TABLE_CONFIGS = STABELCOIN_CONFIGS + ASSETS_CONFIGS


def update_single_table(db_url, table_name, asset, metric, base_params, lookback_days=1):
    engine = create_engine(db_url)
    insp = inspect(engine)
    # 1. 查数据库最后时间
    with engine.connect() as conn:
        last_date = conn.execute(
            text(f"SELECT MAX(date) FROM {table_name}")
        ).scalar()

    if last_date is None:
        start_date = (datetime.now() - timedelta(days=lookback_days)).date()
    else:
        start_date = pd.to_datetime(last_date) - timedelta(days=lookback_days)

    end_date = datetime.now().date()

    # 2. 组合 API 参数（CryptoQuant 用 from/to）
    params = base_params.copy()
    params["from"] = start_date.strftime("%Y%m%d")  # CryptoQuant API 要求日期字符串
    params["to"] = end_date.strftime("%Y%m%d")

    # 3. 调用 API
    df = fetch_cryptoquant_data(asset, metric, params)
    if df is None or df.empty:
        return {"table": table_name, "status": "ok", "inserted": 0}
    if last_date:
        df = df[df["date"] > last_date]
    df["asset"] = asset
    extra_params = {k: v for k, v in base_params.items() if k != "window"}
    for k, v in extra_params.items():
        df[k] = v

    # 4. 插入
    with engine.begin() as conn:
        if insp.has_table(table_name):
            # 对齐表结构
            table_cols = [c["name"] for c in insp.get_columns(table_name)]
            for col in table_cols:
                if col not in df.columns:
                    df[col] = None
            df = df[table_cols]
            df.to_sql(table_name, conn, if_exists="append", index=False, method="multi")
        else:
            df.to_sql(table_name, conn, if_exists="replace", index=False, method="multi")

    return {
        "table": table_name,
        "status": "ok",
        "inserted": len(df),
        "date_range": [str(df["date"].min()), str(df["date"].max())]
    }


def update_all_tables(db_url, configs, lookback_days=1):
    results = []
    total = len(configs)
    for i, cfg in enumerate(configs, start=1):
        logging.info(f"[{i}/{total}] 开始更新表 {cfg['table_name']}")
        try:
            res = update_single_table(
                db_url=db_url,
                table_name=cfg["table_name"],
                asset=cfg["asset"],
                metric=cfg["metric"],
                base_params=cfg["params"],
                lookback_days=lookback_days
            )
        except Exception as e:
            res = {"table": cfg["table_name"], "status": "error", "message": str(e)}
        results.append(res)
        logging.info(f"[{i}/{total}] 完成表 {cfg['table_name']} → {res['status']}")
    return results


if __name__ == '__main__':

    update_all_tables(DB_URL, TABLE_CONFIGS)