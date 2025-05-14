import os
import json
import requests
from random import choice
from utils import log_info, save_to_mongo, get_mongo_db, update_mongo, create_session

GRAPHQL_URL = "https://web.np.playstation.com/api/graphql/v1"
# Persisted query for fetching games list (categoryGridRetrieve)
CATEGORY_QUERY_TEMPLATE = {
    "operationName": "categoryGridRetrieve",
    "variables": {
        "id": "44d8bb20-653e-431e-8ad0-c0a365f68d2f",  # catalog ID for Games
        "pageArgs": {"size": 24, "offset": 0},
        "sortBy": {"name": "productReleaseDate", "isAscending": False},
        "filterBy": [],
        "facetOptions": []
    },
    "extensions": {
        "persistedQuery": {
            "version": 1,
            "sha256Hash": "9845afc0dbaab4965f6563fffc703f588c8e76792000e8610843b8d3ee9c4c09"
        }
    }
}

n_processes = 200  # Adjust based on your system's performance
PLAYSTATION_URL = "https://store.playstation.com/en-us/pages/browse/1"

# Load proxies from file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(BASE_DIR, "proxies.txt"), "r") as f:
    PROXIES = [line.strip() for line in f if line.strip()]

chunk_size = (len(PROXIES) + n_processes - 1) // n_processes
proxy_chunks = [PROXIES[i * chunk_size:(i + 1) * chunk_size] for i in range(n_processes)]

# Optional: persisted query template for fetching *single* product details.
PRODUCT_QUERY_TEMPLATE = {
    "operationName": "productRetrieve",
    "variables": {"productId": None},
    "extensions": {
        "persistedQuery": {
            "version": 1,
            "sha256Hash": "<REAL_PRODUCT_RETRIEVE_SHA256_HASH>"
        }
    }
}

def fetch_playstation_games(offset: int = 0, size: int = 24) -> list[dict]:
    """
    Fetch a page of games via PlayStation Store GraphQL endpoint.
    """
    # prepare payload with updated pageArgs
    payload = CATEGORY_QUERY_TEMPLATE.copy()
    payload["variables"] = payload["variables"].copy()
    payload["variables"]["pageArgs"] = {"size": size, "offset": offset}

    # pick random proxy if available
    proxy = choice(PROXIES) if PROXIES else None
    session = create_session(proxy)

    # send POST request to JSON endpoint
    resp = session.post(GRAPHQL_URL, json=payload, timeout=15)
    resp.raise_for_status()  # raise on HTTP errors
    data = resp.json()
    # extract list of game items
    items = data["data"]["categoryGrid"]["products"]["items"]
    return items

def fetch_playstation_game_by_title(title: str, region: str = "en-us") -> dict | None:
    """
    Fetch a single game's full details by title via GraphQL persisted queries.
    """
    # copy template and set productId when found
    product_query = PRODUCT_QUERY_TEMPLATE.copy()
    product_query["variables"] = product_query["variables"].copy()
        
    # first, page through catalog to find matching title
    offset = 0
    session = create_session(choice(PROXIES) if PROXIES else None)
    while True:
        items = fetch_playstation_games(offset=offset, size=24)
        if not items:
            break
        # search in current page
        for item in items:
            if item.get("name", "").strip().lower() == title.lower():
                product_query["variables"]["productId"] = item["productId"]
                resp = session.post(GRAPHQL_URL, json=product_query, timeout=15)
                resp.raise_for_status()
                detail = resp.json()["data"]["product"]
                # save and return
                db = get_mongo_db()
                save_to_mongo(get_mongo_db(), "playstation_games", detail)
                return detail
        offset += len(items)
    log_info(f"PlayStation: Game not found for title: {title}")
    return None

def main():
    log_info("Starting bulk fetch of PlayStation games via GraphQL...")
    all_games = []
    offset = 0
    while True:
        page = fetch_playstation_games(offset=offset, size=24)
        if not page:
            break
        all_games.extend(page)
        offset += len(page)
    log_info(f"Fetched total {len(all_games)} games.")
    # save to Mongo in bulk (upsert)
    db = get_mongo_db()
    for game in all_games:
        save_to_mongo(db, "playstation_games", game)
    update_mongo(db, "playstation_games")
    log_info("All PlayStation games saved.")

if __name__ == "__main__":
    log_info(fetch_playstation_game_by_title('Minecraft'))
