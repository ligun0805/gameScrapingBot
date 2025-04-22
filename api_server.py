import os
import sys
import psutil
import subprocess
from datetime import timedelta
from dotenv import load_dotenv
from flask_cors import CORS
from flask import Flask, jsonify, send_file, request
from flask_pymongo import PyMongo
from flask_jwt_extended import JWTManager, create_access_token, verify_jwt_in_request
from flask_swagger_ui import get_swaggerui_blueprint
from threading import Thread
from dotenv import load_dotenv
from urllib.parse import urljoin
from utils import log_info, steam_is_friend, steam_send_invite, steam_purchase

from scraper_nintendo import fetch_nintendo_game_by_title
from scraper_playstation import fetch_playstation_game_by_title
from scraper_steam import fetch_steam_game_by_title
from scraper_xbox import fetch_xbox_game_by_title

# Flask app initialization
app = Flask(__name__)
CORS(app)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load environment variables
load_dotenv()
app.config["MONGO_URI"] = os.getenv("MONGO_URI") + "test"

# dotenv variables
access_ip = os.getenv("access_ip", "127.0.0.1")
server_port = os.getenv("server_port", "5000")
username = os.getenv("admin", "admin")
password = os.getenv("password", "password123")
static_token = os.getenv("STATIC_ACCESS_TOKEN", "land33")

# Initialize PyMongo and JWT
mongo = PyMongo(app)

# Custom token verification without Bearer prefix
def custom_token_verification():
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return jsonify({"msg": "Missing Authorization Header"}), 401
    if auth_header != static_token:
        return jsonify({"msg": "Invalid token"}), 401
    
# Swagger configuration without Bearer prefix
swagger_config = {
    "swagger": "2.0",
    "info": {
        "title": "Game Scraping API",
        "description": "API for managing game scrapers and retrieving game details.",
        "version": "1.0.0"
    },
    "host": f"{access_ip}:{server_port}",
    "basePath": "/",
    "schemes": ["http"],
    "securityDefinitions": {
        "TokenAuth": {
            "type": "apiKey",
            "name": "Authorization",
            "in": "headers",
            "description": "Enter your token without the 'Bearer ' prefix"
        }
    },
    "security": [
        {
            "TokenAuth": []
        }
    ],
    "paths": {
        "/login": {
            "post": {
                "summary": "Login",
                "description": "Authenticate user and generate a JWT token.",
                "parameters": [
                    {
                        "name": "body",
                        "in": "body",
                        "required": True,
                        "schema": {
                            "type": "object",
                            "properties": {
                                "username": {
                                    "type": "string",
                                    "example": "admin"
                                },
                                "password": {
                                    "type": "string",
                                    "example": "password123"
                                }
                            }
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Token generated successfully."
                    },
                    "401": {
                        "description": "Invalid credentials."
                    }
                }
            }
        },
        "/games": {
            "get": {
                "summary": "Get Games",
                "description": "Retrieve a paginated list of games from the database.",
                "parameters": [
                    {
                        "name": "page",
                        "in": "query",
                        "type": "integer",
                        "required": False,
                        "default": 1
                    },
                    {
                        "name": "per_page",
                        "in": "query",
                        "type": "integer",
                        "required": False,
                        "default": 10
                    },
                    {
                        "name": "service",
                        "in": "query",
                        "type": "string",
                        "enum": [
                            "steam",
                            "xbox",
                            "playstation",
                            "nintendo"
                        ]
                    },
                    {
                        "name": "region",
                        "in": "query",
                        "type": "string"
                    }
                ],
                "security": [
                    {
                        "TokenAuth": []
                    }
                ],
                "responses": {
                    "200": {
                        "description": "A paginated list of games.",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "games": {
                                    "type": "array",
                                    "items": {
                                        "type": "object"
                                    }
                                }
                            }
                        }
                    },
                    "401": {
                        "description": "Unauthorized."
                    }
                }
            }
        },
        "/scheduler/start": {
            "post": {
                "summary": "Start Scheduler",
                "description": "Start the game scraper scheduler process.",
                "security": [
                    {
                        "TokenAuth": []
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Scheduler started successfully."
                    },
                    "500": {
                        "description": "Error occurred while starting the scheduler."
                    }
                }
            }
        },
        "/scheduler/stop": {
            "post": {
                "summary": "Stop Scheduler",
                "description": "Stop the game scraper scheduler process.",
                "security": [
                    {
                        "TokenAuth": []
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Scheduler stopped successfully."
                    },
                    "404": {
                        "description": "Scheduler not running."
                    },
                    "500": {
                        "description": "Error occurred while stopping the scheduler."
                    }
                }
            }
        },
        "/scheduler/status": {
            "post": {
                "summary": "Scheduler Status",
                "description": "Check if the game scraper scheduler process is running.",
                "security": [
                    {
                        "TokenAuth": []
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Scheduler status retrieved successfully."
                    },
                    "500": {
                        "description": "Error occurred while checking the scheduler status."
                    }
                }
            }
        },
        "/games/count": {
            "get": {
                "summary": "Get Game Count",
                "description": "Retrieve the count of games for a specific service.",
                "parameters": [
                    {
                        "name": "service",
                        "in": "query",
                        "type": "string",
                        "enum": [
                            "steam",
                            "xbox",
                            "playstation",
                            "nintendo"
                        ],
                        "required": True
                    }
                ],
                "security": [
                    {
                        "TokenAuth": []
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Game count retrieved successfully."
                    },
                    "400": {
                        "description": "Invalid service."
                    }
                }
            }
        },
        "/game": {
            "get": {
                "summary": "Update Game by Title",
                "description": "Update game details by title.",
                "parameters": [
                    {
                        "name": "title",
                        "in": "query",
                        "type": "string",
                        "required": True
                    },
                    {
                        "name": "service",
                        "in": "query",
                        "type": "string",
                        "enum": [
                            "steam",
                            "xbox",
                            "playstation",
                            "nintendo"
                        ],
                        "required": True
                    },
                    {
                        "name": "region",
                        "in": "query",
                        "type": "string",
                        "required": True
                    }
                ],
                "security": [
                    {
                        "TokenAuth": []
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Game details retrieved successfully."
                    },
                    "400": {
                        "description": "Invalid service."
                    },
                    "404": {
                        "description": "Game not found."
                    }
                }
            }
        },
        "/logs": {
            "get": {
                "summary": "Fetch Logs",
                "description": "Retrieve the scraper logs file.",
                "security": [
                    {
                        "TokenAuth": []
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Logs retrieved successfully."
                    },
                    "500": {
                        "description": "Error occurred while fetching logs."
                    }
                }
            }
        },
        "/steam/invite": {
            "post": {
                "summary": "Invite Friend",
                "description": "Send a friend invite to a Steam user.",
                "parameters": [
                    {
                        "name": "body",
                        "in": "body",
                        "required": True,
                        "schema": {
                            "type": "object",
                            "properties": {
                                "profile_link": {
                                    "type": "string",
                                    "example": "https://steamcommunity.com/profiles/id"
                                }
                            }
                        }
                    }
                ],
                "security": [
                    {
                        "TokenAuth": []
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Invite sent successfully."
                    },
                    "400": {
                        "description": "Missing profile link."
                    }
                }
            }
        },
        "/steam/check": {
            "post": {
                "summary": "Check Friend",
                "description": "Check if a Steam user is a friend.",
                "parameters": [
                    {
                        "name": "body",
                        "in": "body",
                        "required": True,
                        "schema": {
                            "type": "object",
                            "properties": {
                                "profile_link": {
                                    "type": "string",
                                    "example": "https://steamcommunity.com/profiles/id"
                                }
                            }
                        }
                    }
                ],
                "security": [
                    {
                        "TokenAuth": []
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Friend status checked successfully."
                    },
                    "400": {
                        "description": "Missing profile link."
                    }
                }
            }
        },
        "/steam/purchase": {
            "post": {
                "summary": "Purchase Game",
                "description": "Initiate a game purchase on Steam.",
                "parameters": [
                    {
                        "name": "body",
                        "in": "body",
                        "required": True,
                        "schema": {
                            "type": "object",
                            "properties": {
                                "title": {
                                    "type": "string",
                                    "example": "Game Title"
                                },
                                "friend": {
                                    "type": "string",
                                    "example": "Friend Username"
                                }
                            }
                        }
                    }
                ],
                "security": [
                    {
                        "TokenAuth": []
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Purchase initiated successfully."
                    },
                    "400": {
                        "description": "Missing game title or friend username."
                    }
                }
            }
        },
    },
}

# The Swagger JSON will be publicly accessible, but other endpoints will require authentication
@app.route("/swagger.json")
def swagger_json():
    return jsonify(swagger_config)

# Swagger UI blueprint
SWAGGER_URL = '/swagger'
API_URL = '/swagger.json'

swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={'app_name': "Game Scraping API"}
)

app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

# Check if scheduler.py is running
def is_scheduler_running():
    for proc in psutil.process_iter(attrs=['cmdline']):
        try:
            cmd = proc.info.get('cmdline') or []
            if any("scheduler.py" in part for part in cmd):
                return True
        except (psutil.NoSuchProcess, KeyError):
            continue
    return False

# Routes
@app.route('/scheduler/status', methods=['POST'])
def check_scheduler_status():
    auth_result = custom_token_verification()
    if isinstance(auth_result, tuple):
        return auth_result
    if is_scheduler_running():
        return jsonify({"running": True}), 200
    else:
        return jsonify({"running": False}), 200

@app.route('/scheduler/start', methods=['POST'])
def start_scheduler():
    auth_result = custom_token_verification()
    if isinstance(auth_result, tuple):
        return auth_result
    if is_scheduler_running():
        return jsonify({"msg": "The scheduler is already running on the server."}), 400
    try:
        log_info("******************** Started Scheduler... ********************")
        python_exec = sys.executable
        scheduler_path = os.path.join(BASE_DIR, "scheduler.py")
        subprocess.Popen([python_exec, scheduler_path], cwd=BASE_DIR)
        return jsonify({"msg": "Scheduler started"}), 200
    except Exception as e:
        return jsonify({"msg": f"Error starting scheduler: {e}"}), 500

@app.route('/scheduler/stop', methods=['POST'])
def stop_scheduler():
    auth_result = custom_token_verification()
    if isinstance(auth_result, tuple):
        return auth_result
    if not is_scheduler_running():
        return jsonify({"msg": "Scheduler not running."}), 404
    try:
        scheduler_stopped = False
        for proc in psutil.process_iter(attrs=['pid', 'cmdline']):
            try:
                cmdline = proc.info.get('cmdline') or []
                if any("scheduler.py" in part for part in cmdline):
                    # Kill the scheduler process
                    proc.terminate()
                    proc.wait(timeout=5)
                    scheduler_stopped = True
                    log_info("******************** Killed Scheduler ********************")
                    # Terminate child processes
                    try:
                        children = proc.children(recursive=True)
                        for child in children:
                            try:
                                child.terminate()
                                child.wait(timeout=5)
                            except psutil.NoSuchProcess:
                                pass
                            except Exception as e:
                                print(f"API_server/Stop scheduler : Error terminating child process {child.pid}: {e}")
                    except psutil.NoSuchProcess:
                        pass
                    except Exception as e:
                        print(f"API_server/Stop scheduler : Error retrieving child processes: {e}")
                    break
            except psutil.NoSuchProcess:
                pass
            except Exception as e:
                print(f"API_server/Stop scheduler : Error processing scheduler process {proc.pid}: {e}")

        if scheduler_stopped:
            return jsonify({"msg": "Scheduler and its subprocesses stopped"}), 200
        else:
            return jsonify({"msg": "Scheduler not running"}), 404
    except Exception as e:
        return jsonify({"msg": f"Error stopping scheduler: {str(e)}"}), 500

@app.route('/games/count', methods=['GET'])
def get_game_count():
    auth_result = custom_token_verification()
    if isinstance(auth_result, tuple):
        return auth_result
    service = request.args.get('service')

    if service == "steam":
        collection = mongo.db.steam_games
    elif service == "xbox":
        collection = mongo.db.xbox_games
    elif service == "playstation":
        collection = mongo.db.playstation_games
    elif service == "nintendo":
        collection = mongo.db.nintendo_games
    else:
        return jsonify({"msg": "Invalid service"}), 400
    
    count = collection.count_documents({})
    return jsonify({"count": count}), 200

@app.route('/logs', methods=['GET'])
def fetch_logs():
    auth_result = custom_token_verification()
    if isinstance(auth_result, tuple):
        return auth_result
    try:
        log_path = os.path.join(BASE_DIR, "scraper.log")
        if not os.path.exists(log_path):
            return jsonify({"msg": "Log file not found"}), 404
        return send_file("scraper.log", mimetype="text/plain")
    except Exception as e:
        return jsonify({"msg": f"Error fetching logs: {e}"}), 500

@app.route('/login', methods=['POST'])
def login():
    uname = request.json.get("username")
    pwd = request.json.get("password")
    
    if uname == username and pwd == password:
        return jsonify(access_token=static_token), 200
    else:
        return jsonify({"msg": "Invalid credentials"}), 401
    
@app.route('/games', methods=['GET'])
def get_games():
    # Token verification
    auth = custom_token_verification()
    if isinstance(auth, tuple):
        return auth

    # Parameters
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    service = request.args.get('service')
    region = request.args.get('region')

    # Filters by region
    filters = {}
    if region:
        filters[f"prices.{region}"] = {"$ne": "Free or Not Available"}

    # If specific service requested
    if service in ["steam", "xbox", "playstation", "nintendo"]:
        collection = mongo.db[f"{service}_games"]
        games = paginate(collection, page, per_page, filters)
        for game in games:
            game['service'] = service

    # Merge all services when no specific service
    else:
        try:
            services = ["steam", "xbox", "playstation", "nintendo"]
            all_games = []
            for svc in services:
                collection = mongo.db[f"{svc}_games"]    # fetch all games regardless of region
                docs = list(collection.find({}, {"_id": 0}))
                for game in docs:
                    game['service'] = svc               # assign service flag
                    all_games.append(game)

            # Remove duplicates by (service, title)
            unique = {}
            for game in all_games:
                key = f"{game['service']}:{game.get('title','')}"
                unique.setdefault(key, game)
            deduped = list(unique.values())

            # Pagination on deduped list
            start = (page - 1) * per_page
            if start >= len(deduped):
                return jsonify({
                    "games": [],
                    "has_next": False,
                    "has_prev": page > 1,
                    "page": page,
                    "per_page": per_page,
                    "total": len(deduped)
                }), 200
            games = deduped[start:start + per_page]

        except Exception as e:
            log_info(f"Error in combined query: {e}")
            return jsonify({"msg": f"Error fetching all services: {e}"}), 500

    # Prices by region handling
    for game in games:
        if region and 'prices' in game and region in game['prices']:
            game['region'] = region
            game['price'] = game['prices'].pop(region)
        elif 'prices' in game:
            game['regions'] = list(game['prices'].keys())

    return jsonify({
        "games": games,
        "has_next": len(games) == per_page,
        "has_prev": page > 1,
        "has_next": len(games) == per_page,
        "page": page,
        "per_page": per_page,
        "total": len(games)
    }), 200

@app.route('/game', methods=['GET'])
def update_game():
    auth_result = custom_token_verification()
    if isinstance(auth_result, tuple):
        return auth_result
    title = request.args.get('title')
    service = request.args.get('service')
    region = request.args.get('region')
    
    try:
        if service == 'steam':
            game = fetch_steam_game_by_title(title)
        elif service == 'xbox':
            game = fetch_xbox_game_by_title(title)
        elif service == 'playstation':
            game = fetch_playstation_game_by_title(title)
        elif service == 'nintendo':
            game = fetch_nintendo_game_by_title(title)
        else:
            return jsonify({"msg": "Invalid service"}), 400
    except Exception as e:
        return jsonify({"msg": f"Error fetching game from {service}: {e}"}), 500
    
    if not game:
        return jsonify({"msg": "Game not found"}), 404
    
    game['service'] = service
    game['region'] = region
    return jsonify(game), 200

@app.route('/steam/invite', methods=['POST'])
def invite_friend():
    auth_result = custom_token_verification()
    if isinstance(auth_result, tuple):
        return auth_result
    
    profile_link = request.json.get("profile_link")
    
    
    if not profile_link:
        return jsonify({"msg": "Missing profile link"}), 400
    
    try:
        steam_send_invite(profile_link)
    except:
        return jsonify({"msg": "Invite can not be sent", "link": ""}), 200
    
    return jsonify({"msg": "Invite sent", "link": profile_link + '/friends/pending'}), 200

@app.route('/steam/check', methods=['POST'])
def check_friend():
    auth_result = custom_token_verification()
    if isinstance(auth_result, tuple):
        return auth_result
    
    profile_link = request.json.get("profile_link")
    
    if not profile_link:
        return jsonify({"msg": "Missing profile link"}), 400
    
    is_friend = steam_is_friend(profile_link)
    
    return jsonify({"is_friend": is_friend}), 200

@app.route('/steam/purchase', methods=['POST'])
def purchase_game():
    auth_result = custom_token_verification()
    if isinstance(auth_result, tuple):
        return auth_result
    
    title = request.json.get("title")
    friend = request.json.get("friend")
    
    if not title or not friend:
        return jsonify({"msg": "Missing game title or friend username"}), 400
    
    try:
        steam_purchase(title, friend)
    except Exception as e:
        return jsonify({"msg": "This game can not be delivered because of some reason"}), 200
    
    return jsonify({"msg": "Purchase initiated"}), 200

# Helper function to paginate results
def paginate(collection, page, per_page, filters=None):
    query = {}
    if filters:
        query = filters
    results = list(collection.find(query, {"_id": 0}).skip((page - 1) * per_page).limit(per_page))
    return results

if __name__ == '__main__':
    app.run(host=access_ip, port=server_port, debug=False)