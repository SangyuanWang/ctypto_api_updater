#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: smangj
2025/8/28
mail: 631535207@qq.com
"""
import pandas as pd
import requests

API_KEY = "qYh3Ex0xTVt4I1nmQmDap2iQCZxunclIT7TL1mcCnlTExjV8Q2UpTKVkoqmpLERTLOLgaMFhOhcgb9530JCh9oPG"
ASSETS = ["btc", "eth"]
"""list没完善"""
ASSET_TOKEN = {"btc": [], "eth": [],
               "stablecoin": ["tusd", "gusd", "usdp", "susd", "dai", "usdk", "usdt_eth", "husd", "all_token", "usdc", "sai", "busd"],
               "erc20": ["cqt", "mkr", "prom", "ach", "shib", "agld", "high", "vgx", "sushi", "uma", "alcx", "dar",
                         "ogn", "ern", "psp", "okb", "temple", "poly", "ftt", "grt", "bal", "lqty", "dht", "mta", "pla",
                         "efi", "armor", "ice", "alice", "yfi", "bnt", "bat", "gtc", "dego", "lrc", "jasmy", "ilv",
                         "sand", "matic", "ocean", "cro", "gala", "ghst", "kcs", "uni", "zrx", "hmt", "mana",
                         "aave", "omg", "mask", "dodo", "gno", "crv", "amp", "gt", "ht", "knc", "gods", "cream",
                         "raca", "skl", "hot", "1inch", "comp", "enj", "chz", "rbn", "ankr", "imx", "slp", "storj",
                         "blz", "wbtc", "ens", "link", "dydx", "axs", "ygg"],
               "klay": [], "xrp": [], "alt": ["algo"]}

EXCHANGE = ["binance", "spot_exchange", "derivative_exchange", "binance_us", "ftx", "poloniex"]

def fetch_cryptoquant_data(asset, metric, params):
    url = f"https://api.cryptoquant.com/v1/{asset}/{metric}"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    resp = requests.get(url, headers=headers, params=params)
    # resp = requests.get(url, headers=headers)
    try:
        resp.raise_for_status()
        data = resp.json().get("result", [])
        data = data.get("data", [])
        return pd.DataFrame(data)
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
        print(f"Response content: {resp.content}")
        return None

