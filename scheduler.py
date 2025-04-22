import time
import os
import platform
import subprocess
from utils import log_info

# Define the scraper order and their respective wait intervals (in seconds)
SCRAPER_ORDER = [
    ("scraper_nintendo.py", 10),
    ("scraper_playstation.py", 10),
    ("scraper_xbox.py", 10),
    ("scraper_steam.py", 10),
]

def run_scraper(scraper, interval):
    try:
        log_info(f"========== Starting {scraper}... ==========")

        if platform.system() == "Windows":
            # On Windows, use CREATE_NEW_PROCESS_GROUP
            proc = subprocess.Popen(
                ["python", scraper],
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
            )
        else:
            # On Unix-based systems, use os.setsid()
            proc = subprocess.Popen(
                ["python3", scraper],  # Use "python3" for Unix-based systems
                preexec_fn=os.setsid
            )

        log_info(f"Process {scraper} started with PID {proc.pid}")

        proc.wait()

        log_info(f"========== Finished {scraper} and Updated db. ==========")

    except Exception as e:
        print(f"Scheduler.py : Error running {scraper}: {e}")
        time.sleep(60)  # Wait before retrying in case of an error

    time.sleep(interval)

def main():
    while True:        
        for scraper, interval in SCRAPER_ORDER:
            run_scraper(scraper, interval)

if __name__ == "__main__":
    main()
