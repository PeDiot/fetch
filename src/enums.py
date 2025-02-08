N_ITEMS_MAX = 960


PROJECT_ID = "carlia"
DATASET_ID = "vinted"

CATALOG_TABLE_ID = "catalog"
ITEM_TABLE_ID = "item"
IMAGE_TABLE_ID = "image"
CATEGORY_TABLE_ID = "category"
LIKES_TABLE_ID = "likes"

STAGING_ITEM_TABLE_ID = "item_staging"
STAGING_IMAGE_TABLE_ID = "image_staging"

CATALOG_FIELDS = ["id", "title", "code", "url", "women"]
VALID_FILTER_KEYS = ["brand", "color", "material", "pattern"]


DESIGNER_CATALOG_IDS = [2984, 2985, 2986, 2987, 2990, 2991, 2992]
VINTAGE_BRAND_ID = 14803

BS4_PARSER = "html.parser"
REQUESTS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

SOLD_CONTAINER_TYPE = "div"
SOLD_CONTAINER_ATTRS = {"data-testid": "item-status--content"}
SOLD_STATUS_CONTENT = "Vendu"

NOT_FOUND_CONTAINER_TYPE = "h1"
NOT_FOUND_CONTAINER_ATTRS = {
    "class": "web_ui__Text__text web_ui__Text__heading web_ui__Text__center"
}
NOT_FOUND_STATUS_CONTENT = "La page n'existe pas"