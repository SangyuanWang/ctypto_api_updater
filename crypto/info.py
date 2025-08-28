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


def fetch_cryptoquant_data(asset, metric, start, end, window="day", limit=366):
    url = f"https://api.cryptoquant.com/v1/{asset}/{metric}"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    params = {
        "window": window,
        "from": start,
        "to": end,
        "limit": limit
    }
    resp = requests.get(url, headers=headers, params=params)
    # resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json().get("result", [])
    data = data.get("data", [])
    return pd.DataFrame(data)
