from manager import scraper_manager
import os
from dotenv import load_dotenv

load_dotenv()

scraper_params = {
    "USER_ID": os.environ.get("SHARECHAT_USER_ID"),
    "PASSCODE": os.environ.get("SHARECHAT_PASSWORD"),
    "tag_hashes": ["5anPZA"],
    "bucket_ids": [],
    "content_to_scrape": "trending",
    "pages": 1,
    "unix_timestamp": "",
    "data_path": "",
    "mode": "local",
    "targeting": "tag",
    "is_cron_job": True,
}
print("\nStarting test\nSHARECHAT_USER_ID =", scraper_params["USER_ID"])
print("SHARECHAT_PASSWORD =", scraper_params["PASSCODE"], "\n")

if __name__ == "__main__":
    scraper_manager(scraper_params)
