import multiprocessing
import requests
import os
from random import choice
from requests.adapters import HTTPAdapter
from utils import save_to_mongo, get_mongo_db, update_mongo, log_info, regions_steam
import itertools

n_processes = 100  # Define number of processes
STEAM_API_URL = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"

# Load proxies efficiently
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROXY_FILE = os.path.join(BASE_DIR, "proxies.txt")
with open(PROXY_FILE, "r") as f:
    PROXIES = [line.strip() for line in f if line.strip()]
proxy_pool = itertools.cycle(PROXIES)  # Efficient round-robin proxy cycling

# Set up a requests session with proxy
def create_session(proxy):
    session = requests.Session()

    if proxy:
        session.proxies = {"http": proxy, "https": proxy}

    session.headers.update({"User-Agent": "Mozilla/5.0"})  # Add a user agent for better response handling
    session.mount('https://', HTTPAdapter(max_retries=3))  # Retry on failure
    return session

def fetch_steam_apps(session):
    try:
        response = session.get(STEAM_API_URL, timeout=15)
        response.raise_for_status()
        return response.json().get("applist", {}).get("apps", [])
    except requests.RequestException as e:
        print(f"Failed to fetch app list : {e}")
        return []

def fetch_steam_game_by_title(title: str, region: str = "ru") -> dict | None:
    """Fetch a single Steam game via Store Search API instead of full AppList scan."""
    proxy = choice(PROXIES)
    session = create_session(proxy)
    try:
        # Use the fast search endpoint for a single result
        resp = session.get(
            "https://store.steampowered.com/api/storesearch",
            params={"term": title, "cc": region, "l": region, "count": 1},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("total", 0) == 0:
            return None
        app_id = data["items"][0]["id"]
        game_data = fetch_game_details(app_id, session, region)
        if "error" not in game_data:
            db = get_mongo_db()
            save_to_mongo(db, "steam_games", game_data)
            return game_data
    except requests.RequestException as e:
        print(f"Error searching Steam: {e}")
    return None

def fetch_game_details(app_id, session, region: str = "ru"):
    base_url = "https://store.steampowered.com/api/appdetails"
    try:
        response = session.get(
            base_url,
            params={"appids": app_id, "cc": region, "l": region},  
            timeout=15
        )
        response.raise_for_status()
        data = response.json()

        if str(app_id) not in data or not data[str(app_id)]["success"]:
            return {"error": f"Game {app_id} details not available"}

        game_info = data[str(app_id)]["data"]
        prices = {
            region: fetch_price_for_region(app_id, region)
            for region in regions_steam
        }

        return {
            "title": game_info.get("name", "N/A"),
            "categories": [c["description"] for c in game_info.get("categories", [])],
            "short_description": game_info.get("short_description", "N/A"),
            "full_description": game_info.get("detailed_description", "N/A"),
            "screenshots": [s["path_full"] for s in game_info.get("screenshots", [])],
            "header_image": game_info.get("header_image", "N/A"),
            "rating": game_info.get("metacritic", {}).get("score", "N/A"),
            "publisher": ", ".join(game_info.get("publishers", [])),
            "platforms": ", ".join(k for k, v in game_info.get("platforms", {}).items() if v),
            "release_date": game_info.get("release_date", {}).get("date", "N/A"),
            "prices": prices,
            "url": f"https://store.steampowered.com/app/{app_id}/?cc={region}&l={region}"
        }
    except requests.RequestException as e:
        return {"error": str(e)}

def fetch_price_for_region(app_id, region):
    base_url = "https://store.steampowered.com/api/appdetails"
    proxy = next(proxy_pool)
    session = create_session(proxy)
    try:
        response = session.get(base_url, params={"appids": app_id, "cc": region, "l": "ru"}, timeout=10)
        response.raise_for_status()
        data = response.json()

        if str(app_id) in data and data[str(app_id)]["success"]:
            price_info = data[str(app_id)]["data"].get("price_overview")
            return price_info.get("final_formatted", "Free or Not Available") if price_info else "Not Available"
    except requests.RequestException as e:
        print(f"Error fetching price for {app_id} in {region}: {e}")
    return "Not Available"

def process_apps_range(start_index, end_index, apps, proxy):
    session = create_session(proxy)
    db = get_mongo_db()

    for index in range(start_index, end_index):
        app = apps[index]
        try:
            game_data = fetch_game_details(app["appid"], session)
            if "error" not in game_data:
                save_to_mongo(db, "steam_games", game_data)
        except Exception as e:
            print(f"Error processing app {app['appid']}: {e}")

def main():
    proxy_list = list(itertools.islice(proxy_pool, n_processes))  # Get unique proxies for each process

    apps = fetch_steam_apps(create_session(proxy_list[0]))  # Initial fetch using a proxy
    if not apps:
        log_info("No Steam apps found to process.")
        return

    total_apps = len(apps)
    log_info(f"Found {total_apps} games in Steam")
    chunk_size = (total_apps + n_processes - 1) // n_processes
    ranges = [(i * chunk_size, min((i + 1) * chunk_size, total_apps)) for i in range(n_processes)]

    # Use Pool to manage processes efficiently with proxies
    with multiprocessing.Pool(processes=n_processes) as pool:
        pool.starmap(process_apps_range, [(start, end, apps, proxy_list[i]) for i, (start, end) in enumerate(ranges)])

    db = get_mongo_db()
    update_mongo(db, "steam_games")
    log_info("All Steam processes completed.")

if __name__ == "__main__":
    main()
