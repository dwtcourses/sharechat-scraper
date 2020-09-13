from manager import scraper_manager
import os
from dotenv import load_dotenv

load_dotenv()

scraper_params = {
    "USER_ID": os.environ.get("SHARECHAT_USER_ID"),
    "PASSCODE": os.environ.get("SHARECHAT_PASSWORD"),
    "tag_hashes": ["", ""],
    "bucket_ids": ["", ""],
    "content_to_scrape": "virality",
    "pages": "",
    "unix_timestamp": "",
    "virality_job": 2,
    "mode": "archive",
    "targeting": "",
    "is_cron_job": True,
}


if __name__ == "__main__":
    scraper_manager(scraper_params)
