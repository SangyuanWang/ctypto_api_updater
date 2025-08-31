from fastapi import FastAPI, Query

import crypto.updater as cryptoquant_updater
from db_info import DB_URL

# import other_updater_module

UPDATER_MAP = {
    "cryptoquant": cryptoquant_updater,
    # "otherapi": other_updater_module
}

app = FastAPI()

@app.get("/update")
def update_data(
    api_type: str = Query(..., description="要更新的API类型"),
    table_name: str = Query(None, description="要更新的表名，不传则更新API对应的所有表")
):
    """
    支持：
    - /update?api_type=cryptoquant → 更新对应 API 的所有表
    - /update?api_type=cryptoquant&table_name=cryptoquant_inflow_day → 更新单表
    """
    if api_type not in UPDATER_MAP:
        return {"status": "error", "message": f"未知 API 类型 {api_type}"}

    updater_module = UPDATER_MAP[api_type]
    configs = updater_module.TABLE_CONFIGS

    if table_name:
        configs = [cfg for cfg in configs if cfg["table_name"] == table_name]
        if not configs:
            return {"status": "error", "message": f"表 {table_name} 未在 API {api_type} 配置中"}

    results = updater_module.update_all_tables(DB_URL, configs)
    return results
