# 拉取crypto相关API入库
## 部署更新API接口
uvicorn updater:app --host 0.0.0.0 --port xxxx

## 分API
`<api_name>.updater.update_all_tables()`