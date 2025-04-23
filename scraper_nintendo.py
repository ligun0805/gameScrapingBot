import requests
import time
import multiprocessing
import itertools
import json
import functools
from random import choice
from typing import List
from datetime import datetime
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor
from utils import log_info, get_mongo_db, save_to_mongo, update_mongo, regions_nintendo

n_processes = 50

API_URL = "https://searching.nintendo-europe.com/en/select?q=*&fq=type%3AGAME%20AND%20((playable_on_txt%3A%22HAC%22))%20AND%20sorting_title%3A*%20AND%20*%3A*&sort=score%20desc%2C%20date_from%20desc&start=0&rows=100000&wt=json&bf=linear(ms(priority%2CNOW%2FHOUR)%2C3.19e-11%2C0)&bq=!deprioritise_b%3Atrue%5E999" # API endpoint

# Load proxies from file
with open("proxies.txt") as f:
    PROXIES = [line.strip() for line in f if line.strip()]
    
chunk_size = (len(PROXIES) + n_processes - 1) // n_processes
proxy_chunks = [PROXIES[i * chunk_size:(i + 1) * chunk_size] for i in range(n_processes)]

# Set up a requests session with proxy and retry logic
def create_session(proxy=None, timeout=(5, 10)):
    
    session = requests.Session()

    if proxy:
        session.proxies = {"http": proxy, "https": proxy}

    session.mount('https://', HTTPAdapter(max_retries=3))
    session.request = functools.partial(session.request, timeout=timeout)
    return session

def fetch_games():
    from requests.exceptions import Timeout, RequestException
    try:
        session = create_session()
        response = session.get(API_URL)
        response.raise_for_status()
        return response.json()['response']['docs']
    except Timeout:
        print("Nintendo : Timeout fetching games")
        return []
    except RequestException as e:
        print(f"Nintendo : Request error fetching games: {e}")
        return []

def fetch_nintendo_game_by_title(title: str) -> dict | None:
    from requests.exceptions import Timeout, RequestException
    try:
        proxy = choice(PROXIES)
        session = create_session(proxy)
        api = f'https://searching.nintendo-europe.com/en/select?q={title}&fq=type%3AGAME%20AND%20sorting_title%3A*%20AND%20*%3A*&sort=deprioritise_b%20asc%2C%20popularity%20asc&start=0&rows=24&wt=json&bf=linear(ms(priority%2CNOW%2FHOUR)%2C3.19e-11%2C0)&bq=!deprioritise_b%3Atrue%5E999'
        response = session.get(api)
        response.raise_for_status()
        data = response.json()['response']['docs']
        if data and len(data) > 0:
            for game in data:
                if game['title'] == title:
                    game_data = process_nintendo_game(game, proxy)
                    if game_data:
                        db = get_mongo_db()
                        save_to_mongo(db, "nintendo_games", game_data)
                        return game_data
        
        print(f"Nintendo : No game found for title: {title}")
        return None
    except Timeout:
        print(f"Nintendo : Timeout fetching game by title \"{title}\"")
        return None
    except RequestException as e:
        print(f"Nintendo : Request error fetching game by title \"{title}\": {e}")
        return None

def fetch_screenshots(url: str, proxy) -> list:
    session = create_session(proxy)
    resp = session.get(url)
    
    if resp.status_code != 200:
        print(f"Error fetching screenshots: {resp.status_code}")
        return []
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    gallery = soup.find('section', id='Gallery')
    
    if gallery is None:
        return []
    
    images = []
    overlays: List[Tag] = gallery.find_all('div', class_='mediagallery-image-overlay')
    
    for overlay in overlays:
        img_xs: str = overlay.find_next_sibling('img')['data-xs']
        img = img_xs.replace('_TM_Standard', '')
        images.append(img)
    
    return images

def fetch_full_description(url: str, proxy) -> str:
    session = create_session(proxy)
    resp = session.get(url)
    
    if resp.status_code != 200:
        print(f"Error fetching full description: {resp.status_code}")
        return 'N/A'
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    overview = soup.find('section', id='Overview')
    
    if overview is None:
        return 'N/A'
    
    elements: List[Tag] = overview.find_all('div', class_='row-content')
    
    for element in elements:
        images = element.find_all('img')
        for image in images:
            image['src'] = image['data-xs']
        
    return ''.join([str(element) for element in elements])

def fetch_slug(title: str, proxy) -> str | None:
    try:
        session = create_session(proxy)
        
        api = 'https://u3b6gr4ua3-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.23.2)%3B%20Browser'
        headers = {
            'x-algolia-api-key': 'a29c6927638bfd8cee23993e51e721c9',
            'x-algolia-application-id': 'U3B6GR4UA3'
        }
        payload = {"requests":[{"indexName":"store_game_en_us","query":title,"params":"filters=&hitsPerPage=10&analytics=true&facetingAfterDistinct=true&clickAnalytics=true&highlightPreTag=%5E*%5E%5E&highlightPostTag=%5E*&attributesToHighlight=%5B%22description%22%5D&facets=%5B%22*%22%5D&maxValuesPerFacet=100"}]}
        
        resp = session.post(api, headers=headers, json=payload)
        data = resp.json()
        
        if data['results'][0]['nbHits'] == 0:
            return None
    except Exception as e:
        print(f"Error fetching slug: {e}")
        return None
    
    slug = resp.json()['results'][0]['hits'][0]['urlKey']
    
    return slug

def fetch_build_id(slug: str, proxy) -> str | None:
    try:
        session = create_session(proxy)
        
        api = f'https://www.nintendo.com/us/store/products/{slug}/'
        resp = session.get(api)
        soup = BeautifulSoup(resp.text, 'html.parser')
        data = soup.find('script', id='__NEXT_DATA__')
        
        build_id = json.loads(data.text)['buildId']
        
        return build_id
        
    except Exception as e:
        print(f"Error fetching build ID: {e}")
        return None

def fetch_america_price(region: str, slug: str, build_id: str, proxy) -> dict:
    try:
        session = create_session(proxy)
        
        api = f'https://www.nintendo.com/_next/data/{build_id}/{region}/store/products/{slug}.json?slug={slug}'
        resp = session.get(api)
        data = resp.json()
    except Exception as e:
        print(f"Error fetching America price: {e}")
        return (region.split('-')[-1], 'N/A')
    
    try:
        if data['pageProps']['linkedData'][0]['offers']['price'] is None:
            return (region.split('-')[-1], 'N/A')
    except:
        return (region.split('-')[-1], 'N/A')
        
    price = data['pageProps']['linkedData'][0]['offers']['price'] + ' ' + data['pageProps']['linkedData'][0]['offers']['priceCurrency']
    
    return (region.split('-')[-1], price)

def fetch_asia_price(region: str, nsuid: str, proxy) -> dict:
    try:
        session = create_session(proxy)
        api = f'https://ec.nintendo.com/api/{region.upper()}/en/guest_prices?ns_uids={nsuid}'
        resp = session.get(api)
        data = resp.json()
    except Exception as e:
        print(f"Error fetching Asia price: {e}")
        return (region.split('-')[-1], 'N/A')
    
    try:
        if data[0]['sales_status'] != 'onsale':
            return (region.split('-')[-1], 'N/A')
    except:
        return (region.split('-')[-1], 'N/A')
    
    price = data[0]['price']['regular_price']['raw_value'] + ' ' + data[0]['price']['regular_price']['currency']
    
    return (region.split('-')[-1], price)

def fetch_europe_price(region: str, nsuid: str, proxy) -> dict:
    try:
        session = create_session(proxy)
        
        api = f"https://api.ec.nintendo.com/v1/price?country={region.split('-')[-1].upper()}&lang={region.split('-')[0]}&ids={nsuid}"
        resp = session.get(api)
        data = resp.json()
    except Exception as e:
        print(f"Error fetching Europe price: {e}")
        return (region.split('-')[-1], 'N/A')
    
    try:
        if data['prices'][0]['sales_status'] != 'onsale':
            return (region.split('-')[-1], 'N/A')
    except:
        return (region.split('-')[-1], 'N/A')
    
    price = data['prices'][0]['regular_price']['raw_value'] + ' ' + data['prices'][0]['regular_price']['currency']
    
    return (region.split('-')[-1], price)

def fetch_prices(nsuid: str, slug: str | None, build_id: str, proxy) -> dict:
    prices = {}
    
    if slug is None or build_id is None:
        america_prices = []
        for region in regions_nintendo[:8]:
            america_prices.append((region.split('-')[-1], 'N/A'))
    else:
        with ThreadPoolExecutor(max_workers=8) as executor:
            america_prices = executor.map(fetch_america_price, regions_nintendo[:8], [slug] * 8, [build_id] * 8, [proxy] * 8)
        
    with ThreadPoolExecutor(max_workers=1) as executor:
        asia_prices = executor.map(fetch_asia_price, regions_nintendo[8:9], [nsuid] * 1, [proxy] * 1)
        
    with ThreadPoolExecutor(max_workers=11) as executor:
        europe_prices = executor.map(fetch_europe_price, regions_nintendo[9:], [nsuid] * 11, [proxy] * 11)
        
    for region, price in america_prices:
        prices[region] = price
        
    for region, price in asia_prices:
        prices[region] = price
        
    for region, price in europe_prices:
        prices[region] = price
        
    return prices

def process_nintendo_game(game: dict, proxy) -> dict | None: 
    try:
        title = game.get('title', 'N/A')
        categories = game.get('pretty_game_categories_txt', [])
        short_description = game.get('product_catalog_description_s', 'N/A')
        header_image = game.get('image_url_sq_s', 'N/A')
        rating = game.get('pretty_agerating_s', 'N/A')
        publisher = game.get('publisher', 'N/A')
        platforms = game.get('system_names_txt', 'N/A')
        release_date = game.get('date_from', 'N/A')
        
        if release_date != 'N/A':
            release_date = datetime.fromisoformat(release_date.replace("Z", "")).strftime('%B %d, %Y')
            
        raw_path = game.get('url', None)
        
        if raw_path:
            url = 'https://www.nintendo.com' + raw_path
            screenshots = fetch_screenshots(url, proxy)
        else:
            url = None
            screenshots = []
            
        full_description = fetch_full_description(url, proxy)
        
        nsuid = game.get('nsuid_txt', None)
        
        if nsuid:
            slug = fetch_slug(title, proxy)
            build_id = fetch_build_id(slug, proxy)
            prices = fetch_prices(nsuid[0], slug, build_id, proxy)
        else:
            prices = {}
            
            for region in regions_nintendo:
                prices[region.split('-')[-1]] = 'N/A'
        
        game_data = {
            "title": title,                          
            "categories": categories,
            "short_description": short_description,
            "full_description": full_description,
            "screenshots": screenshots,
            "header_image": header_image,
            "rating": rating,
            "publisher": publisher,
            "platforms": platforms,
            "release_date": release_date,
            "prices": prices,
            "url": url
        }
        print(f"Nintendo game: {title}")
        return game_data
    except Exception as e:
        print(f"Error processing Nintendo game: {e}")
        return None

def process_games_range(start_index, end_index, games, proxy_list):
    db = get_mongo_db()
    
    for index in range(start_index, end_index):
        proxy = next(itertools.cycle(proxy_list))

        try:
            game_data = process_nintendo_game(games[index], proxy)
            if game_data:
                save_to_mongo(db, "nintendo_games", game_data)
            else:
                print(f"Missing data for game {index}")
        except Exception as e:
            print(f"Error processing game at index {index}: {e}")

def main():
    log_info("Waiting for fetching Nintendo games...")
    games = fetch_games()

    total_games = len(games)
    if total_games == 0:
        log_info("No games found to process.")
        return
    
    log_info(f"Fetched {total_games} games in Nintendo.")
    chunk_size = (total_games + n_processes - 1) // n_processes
    ranges = [(i * chunk_size, min((i + 1) * chunk_size, total_games)) for i in range(n_processes)]
    
    with multiprocessing.Pool(processes=n_processes) as pool:
        pool.starmap(process_games_range, [(start, end, games, proxy_chunks[i]) for i, (start, end) in enumerate(ranges)])

    db = get_mongo_db()
    update_mongo(db, "nintendo_games")
    log_info("All Nintendo processes completed.")

if __name__ == "__main__":
    main()
    