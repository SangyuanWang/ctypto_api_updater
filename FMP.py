#!/usr/bin/env python
from datetime import datetime, timedelta
from db_info import write_to_db
import pandas as pd
import requests

API_KEY = "Fg717Owfnfye5r2HQVIhDuBTefMJs7uA"
def get_jsonparsed_data(symbol, key, start, end):

    url = f"https://financialmodelingprep.com/api/v3/historical-chart/1hour/{symbol}?apikey={key}&from={start}&to={end}"
    resp = requests.get(url, timeout=10)
    data = resp.json()
    return pd.DataFrame(data)


def fetch_full_data(symbol, key, start_date, end_date):
    all_data = []

    # 将字符串日期转 datetime 对象
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # API 每次最多返回约 2140 条数据，大约 2140 小时 = 89 天左右
    delta = timedelta(hours=2160)

    current_start = start_dt
    while current_start < end_dt:
        current_end = min(current_start + delta, end_dt)
        # 转为字符串
        start_str = current_start.strftime("%Y-%m-%d")
        end_str = current_end.strftime("%Y-%m-%d")
        print(f"Fetching data from {start_str} to {end_str}...")

        try:
            df = get_jsonparsed_data(symbol, key, start_str, end_str).sort_values("date")
        except:
            continue
        finally:
            current_start = current_end + timedelta(days=1)
        all_data.append(df)

    # 拼接所有数据
    if len(all_data) > 0:
        full_df = pd.concat(all_data, ignore_index=True)
    else:
        full_df = pd.DataFrame()
    return full_df
# symbol = "BTCUSD"
# # start_date = "2023-01-01"
# url = (f"https://financialmodelingprep.com/stable/historical-chart/1hour?symbol={symbol}&apikey={API_KEY}")
# # url = (f"https://financialmodelingprep.com/stable/cryptocurrency-list?apikey={API_KEY}")
# data = fetch_full_data(symbol, API_KEY, "2023-01-01", "2025-08-01")
#
# name = f"{symbol}_Hour"
# write_to_db(data, name)
#
# data.to_csv(f"{symbol}.csv")
# print(data)

if __name__ == '__main__':
    all = pd.read_csv("all_data.csv")
    fmp_list = pd.read_csv("symbol_list.csv")
    fmp_list["exchg_symbol"] = fmp_list["symbol"].str.replace("USD", "", case=False).str.lower()
    filter = fmp_list.loc[fmp_list["exchg_symbol"].isin(all["symbol"])]

    start_date = "2023-01-01"
    end_date = datetime.today().strftime("%Y-%m-%d")
    for i, row in filter.iterrows():
        symbol = row["symbol"]
        data = fetch_full_data(symbol, API_KEY, start_date, end_date)
        if not data.empty:
            write_to_db(data, f"FMP_{symbol}_Hour")