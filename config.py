import os
from dotenv import load_dotenv

from manager import scraper_manager

load_dotenv()

# UPDATE AS REQUIRED
scraper_params = {
    "USER_ID": os.environ.get("SHARECHAT_USER_ID"),
    "PASSCODE": os.environ.get("SHARECHAT_PASSWORD"),
    "tag_hashes": ["", ""],  # insert tag hashes as strings
    "bucket_ids": ["", ""],
    "content_to_scrape": "virality",  # select one from: trending / fresh / virality / ml
    "pages": "",  # used when content_to_scrape == trending / fresh / ml
    "unix_timestamp": "",  # 10 digit unix timestamp. used when content_to_scrape == fresh and is_cron_job == False
    "virality_job": 1,  # select from 1 or 2. used when content_to_scrape == virality
    "mode": "archive",  # select from: local / archive
    "targeting": "",  # select from: tag / bucket
    "is_cron_job": True,
}


if __name__ == "__main__":
    scraper_manager(scraper_params)
