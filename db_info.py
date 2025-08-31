#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: smangj
2025/8/28
mail: 631535207@qq.com
"""
from sqlalchemy import create_engine
from sqlalchemy import inspect

DB_URL = "postgresql://postgres:Vk3rUTQjcweSkOGO@abel-sit.cwbe6kuqcn4q.us-east-1.rds.amazonaws.com:5432/abel-test"

def write_to_db(df, table_name, db_url=DB_URL):
    engine = create_engine(db_url)
    # --- 对齐 DataFrame 列到表结构 ---
    insp = inspect(engine)
    table_cols = [col['name'] for col in insp.get_columns(table_name)]

    # 补齐缺失列
    for col in table_cols:
        if col not in df.columns:
            df[col] = None

    # 丢掉多余列
    df = df[table_cols]
    with engine.begin() as co:
        df.to_sql(table_name, co, if_exists="append", index=False, method='multi')
