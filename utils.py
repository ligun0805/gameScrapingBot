import os
import logging
import time
import pickle
import tempfile
from bs4 import BeautifulSoup
from pymongo import MongoClient
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

load_dotenv()

chromedriver_path = os.getenv("chromedriver_path")
chrome_path = os.getenv("chrome_path")

regions_playstation = [
    # 'en-us',
    'en-eu',
    'de-at',
    'es-ar',
    'ar-bh',
    'fr-be',
    'pt-br',
    'en-gb',
    'de-de',
    'en-hk',
    'en-gr',
    'en-in',
    'es-es',
    'it-it',
    'ar-qa',
    'en-kw',
    'ar-lb',
    'de-lu',
    'nl-nl',
    'ar-ae',
    'ar-om',
    'pl-pl',
    'pt-pt',
    "ro-ro",
    'ar-sa',
    'sl-si',
    'sk-sk',
    'tr-tr',
    'fi-fi',
    'fr-fr',
    'en-za'
]

regions_steam = [
        "us",  # United States
        "gb",  # United Kingdom
        "eu",  # European Union
        "jp",  # Japan
        "in",  # India
        "br",  # Brazil
        "au",  # Australia
        "ca",  # Canada
        "ru",  # Russia
        "cn",  # China
        "kr",  # South Korea
        "mx",  # Mexico
        "za",  # South Africa
        "ar",  # Argentina
        "tr",  # Turkey
        "id",  # Indonesia
        "sg",  # Singapore
        "ph",  # Philippines
        "th",  # Thailand
        "my",  # Malaysia
        "nz",  # New Zealand
        "sa",  # Saudi Arabia
        "ae",  # United Arab Emirates
    ]

regions_xbox = [
        # "en-us",  # United States as default
        "en-gb",  # United Kingdom      
        "en-eu",  # European Union      
        "en-in",  # India               
        "pt-br",  # Brazil              
        "en-au",  # Australia           
        "en-ca",  # Canada
        "ru-ru",  # Russia              
        "zh-cn",  # China               
        "es-mx",  # Mexico              
        "en-za",  # South Africa         
        "es-ar",  # Argentina
        "tr-tr",  # Turkey               
        "ar-sa",  # Saudi Arabia         
        "ar-ae",  # United Arab Emirates 
        "en-hu",  # Hungary              
        "es-co",  # Colombia             
        "en-pl",  # Poland              
        "en-no",  # Norway              
    ]

regions_nintendo = [
    'es-ar',
    'pt-br',
    'en-ca',
    'es-cl',
    'es-co',
    'es-mx',
    'es-pe',
    'us',
    'au',
    'nl-be',
    'de-de',
    'es-es',
    'fr-fr',
    'it-it',
    'nl-nl',
    'de-at',
    'pt-pt',
    'de-ch',
    'en-za',
    'en-gb'
]

# Database configuration
def get_mongo_db():
    mongo_uri = os.getenv("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client["test"]
    return db

def update_mongo(db, collection_name):
    db[collection_name].drop()
    db[f"{collection_name}_tmp"].rename(collection_name)

def save_to_mongo(db, collection_name, data):
    collection = db[f"{collection_name}_tmp"]
    # collection.insert_one(data)
    title = data.get("title")
    if title:
        collection = db[collection_name]
        existing_data = collection.find_one({"title" : title})
        if existing_data:
            collection.update_one(
                {"_id": existing_data["_id"]},
                {"$set": data}
            )
        else:
            collection.insert_one(data)

def get_selenium_browser(retries=3):
    options = Options()
    options.add_argument('--no-sandbox')  # Critical for Linux/Docker
    options.add_argument('--disable-dev-shm-usage')  # For limited shared memory
    options.add_argument('--headless=new')  # If running headless
    options.add_argument('--disable-gpu')  # Sometimes needed
    
    
    # Adjust path if needed
    # options.binary_location = chrome_path
    service = Service(
        executable_path=ChromeDriverManager('135').install()
    )
    driver = webdriver.Chrome(service=service, options=options)
    
    return driver

def click_loadmore_btn(browser, btn_dom):
    count = 0
    while True:
        try:
            btn = WebDriverWait(browser, 60).until(
                EC.element_to_be_clickable((By.XPATH, btn_dom))
            )
        except TimeoutException:
            print("Timeout: Load more button not found or not clickable.")
            return browser
        except Exception as e:
            print(f"Error processing game: {e}")
            print("-"*10, "! load more : exception occur : plz check the network !", "-"*10)
            time.sleep(60)
            continue
        btn = browser.find_element(By.XPATH, btn_dom)
        btn.click()
        count += 1
        if(count % 50 == 0):
            print("-"*10, "Load more button", count, " times clikced in Xbox","-"*10)

def search_game(browser, search_dom, result_dom, title):
    try:
        locator = (By.CSS_SELECTOR, search_dom)
        WebDriverWait(browser, 10).until(
            EC.presence_of_all_elements_located(locator)  # Wait for matching element
        )
        search_input = browser.find_elements(*locator)[-1]

        WebDriverWait(browser, 10).until(EC.element_to_be_clickable(search_input))
        search_input.send_keys(title)
        search_input.send_keys(Keys.RETURN)

        locator = (By.CSS_SELECTOR, result_dom)
        WebDriverWait(browser, 10).until(
            EC.visibility_of_all_elements_located(locator)
        )
        soup = BeautifulSoup(browser.page_source, 'html.parser')
        return soup
    except TimeoutException:
        return []
    
# Steam functions
def steam_purchase(title: str, friend: str):
    driver = get_selenium_browser()
    steam_store_login(driver)
    driver.get(f'https://store.steampowered.com/search/?term={title}')
    results = driver.find_elements(By.CLASS_NAME, 'search_result_row')

    item_link = results[0].get_attribute('href')
    driver.get(item_link)

    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, 'Add to Cart'))).click()
    time.sleep(5)
    driver.get('https://store.steampowered.com/cart')
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//button[@class="_2GLDG_XIMaVS7hU2xEFzBo DialogDropDown _DialogInputContainer  Focusable"]'))).click()
    dialog_menu_position = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'DialogMenuPosition')))
    dialog_menu_position.find_elements(By.TAG_NAME, 'button')[-1].click()
    payment_btns = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//button[@class="qV80oahDZsbXiS6lIDLND DialogButton _DialogLayout Primary Focusable"]')))
    payment_btns[-1].click()

    driver.find_element(By.XPATH, '//button[@class="DialogButton _DialogLayout Primary Focusable"]').click()
    
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//input[@class="DialogInput DialogInputPlaceholder DialogTextInputBase _1OuNJQWR-7lSdtgyJf69uF Focusable"]'))).send_keys(friend)
    
    results = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//div[@class="_321Woxp4ONn3k90_NLayE0 _3td3cAnGbbbAOXW8x2pD-j _29WypCpglgRKsR_fMPsoFX Panel Focusable"]')))
    results[0].click()
        
    payment_btns = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//button[@class="qV80oahDZsbXiS6lIDLND DialogButton _DialogLayout Primary Focusable"]')))
    payment_btns[-1].click()
    time.sleep(5)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'accept_ssa'))).click()
    driver.find_element(By.LINK_TEXT, 'Purchase').click()
    time.sleep(3)
    driver.quit()

def steam_send_invite(profile_link: str):
    driver = get_selenium_browser()
    steam_commnunity_login(driver)
    driver.get(profile_link)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, 'Add Friend'))).click()
    time.sleep(3)
    driver.quit()

def steam_is_friend(profile_link: str):
    try:
        driver = get_selenium_browser()
        steam_commnunity_login(driver)
        driver.get(profile_link)
    except Exception as e:
        print(f"Error getting profile: {e}")
    
    try:
        WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.LINK_TEXT, 'Message')))
        driver.quit()
        return True
    except:
        return False

def steam_store_login(driver: webdriver.Chrome):
    try:
        st_username = os.getenv("steam_username")
        st_password = os.getenv("steam_password")
        
        driver.get('https://store.steampowered.com/login')
        inputs = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CLASS_NAME, '_2GBWeup5cttgbTw8FM3tfx')))
        inputs[0].send_keys(st_username)
        inputs[1].send_keys(st_password)
        driver.find_element(By.CLASS_NAME, 'DjSvCZoKKfoNSmarsEcTS').click()
        time.sleep(5)
    except Exception as e:
        print(f"Error logging in: {e}")
        
def steam_commnunity_login(driver: webdriver.Chrome):
    try:
        st_username = os.getenv("steam_username")
        st_password = os.getenv("steam_password")
        
        driver.get('https://steamcommunity.com/login/home/?goto=')
        inputs = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CLASS_NAME, '_2GBWeup5cttgbTw8FM3tfx')))
        inputs[0].send_keys(st_username)
        inputs[1].send_keys(st_password)
        driver.find_element(By.CLASS_NAME, 'DjSvCZoKKfoNSmarsEcTS').click()
        time.sleep(5)
    except Exception as e:
        print(f"Error logging in: {e}")

# Configure logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(
    filename="scraper.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
# Remove unwanted logs from third-party libraries
logging.getLogger().setLevel(logging.INFO)
logging.getLogger('werkzeug').setLevel(logging.CRITICAL)  # Suppress Flask logs
logging.getLogger('urllib3').setLevel(logging.CRITICAL)   # Suppress HTTP requests warnings
logging.getLogger('asyncio').setLevel(logging.CRITICAL)   # Suppress asyncio warnings
logging.getLogger('sqlalchemy').setLevel(logging.CRITICAL)  # Suppress SQLAlchemy warnings

def log_info(message):
    logging.info(message)