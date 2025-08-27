import pandas as pd

import requests

if __name__ == '__main__':
    import requests

    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,  # 每页最大 250
        "page": 1
    }

    all_data = []
    for page in range(1, 5):  # 假设要抓前 1000 个币
        params["page"] = page
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        if not data:  # 没有更多数据时结束
            break
        all_data.extend(data)

    pd.DataFrame(all_data).to_csv("all_data.csv")
    print(f"总共获取了 {len(all_data)} 条数据")
    print(all_data[0])
