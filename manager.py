from sharechat_scrapers import (
    trending_content_scraper,
    fresh_content_scraper,
    ml_scraper,
    virality_scraper,
)
import time
from dotenv import load_dotenv
import logging

load_dotenv()


def scraper_manager(scraper_params):
    try:
        if scraper_params["content_to_scrape"] == "trending":
            trending_content_scraper(
                USER_ID=scraper_params["USER_ID"],
                PASSCODE=scraper_params["PASSCODE"],
                tag_hashes=scraper_params["tag_hashes"],
                bucket_ids=scraper_params["bucket_ids"],
                pages=scraper_params["pages"],
                mode=scraper_params["mode"],
                targeting=scraper_params["targeting"],
            )
        elif scraper_params["content_to_scrape"] == "fresh":
            if scraper_params["is_cron_job"] is True:
                scraper_params["unix_timestamp"] = str(time.time()).split(".")[0]
            else:
                pass
            fresh_content_scraper(
                USER_ID=scraper_params["USER_ID"],
                PASSCODE=scraper_params["PASSCODE"],
                tag_hashes=scraper_params["tag_hashes"],
                bucket_ids=scraper_params["bucket_ids"],
                pages=scraper_params["pages"],
                unix_timestamp=scraper_params["unix_timestamp"],
                mode=scraper_params["mode"],
                targeting=scraper_params["targeting"],
            )
        elif scraper_params["content_to_scrape"] == "virality":
            virality_scraper(
                USER_ID=scraper_params["USER_ID"],
                PASSCODE=scraper_params["PASSCODE"],
                virality_job=scraper_params["virality_job"],
            )
        elif scraper_params["content_to_scrape"] == "ml":
            ml_scraper(
                USER_ID=scraper_params["USER_ID"],
                PASSCODE=scraper_params["PASSCODE"],
                tag_hashes=scraper_params["tag_hashes"],
                bucket_ids=scraper_params["bucket_ids"],
                pages=scraper_params["pages"],
                mode=scraper_params["mode"],
                targeting=scraper_params["targeting"],
            )
        else:
            raise ValueError(
                "Invalid value entered for content_to_scrape. Select one from: trending, fresh, virality, ml"
            )
    except Exception:
        print(logging.traceback.format_exc())
