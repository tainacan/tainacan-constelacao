from hooks.tainacan_api_hook import TainacanAPIHook

hook = TainacanAPIHook(conn_id='brasiliana.local')
data = hook.get_collection_items(
    5, [37, 7], 1)
print(data)
