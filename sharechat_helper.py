# Common helper functions for various Sharechat scrapers
import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from IPython.display import HTML
import time
from random import uniform
import json
import uuid
from dotenv import load_dotenv

import wget
import s3_mongo_helper
import tempfile
import shutil
import subprocess
import logging
import boto3
from pymongo import MongoClient
import sys
import codecs
from collections import Counter

load_dotenv()

# For targeted tag scraper


# Generates params for API requests
def generate_requests_dict(
    USER_ID,
    PASSCODE,
    tag_hash=None,
    content_type=None,
    unix_timestamp=None,
    post_key=None,
    bucket_id=None,
):
    requests_dict = {
        "tag_data_request": {  # gets tag info
            "api_url": "https://apis.sharechat.com/explore-service/v1.0.0/tag?tagHash=",
            "headers": {
                "content-type": "application/json",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                "x-sharechat-authorized-userid": USER_ID,
                "x-sharechat-secret": PASSCODE,
                "x-sharechat-userid": USER_ID,
            },
        },
        "trending_posts_request": {  # gets media & metadata from trending section within tag
            "body": {
                "bn": "broker3",
                "userId": USER_ID,
                "passCode": PASSCODE,
                "client": "web",
                "message": {"th": "{}".format(tag_hash), "allowOffline": True},
            },
            "api_url": "https://restapi1.sharechat.com/getViralPostsSeo",
            "headers": {
                "content-type": "application/json",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                "x-sharechat-authorized-userid": USER_ID,
                "x-sharechat-secret": PASSCODE,
                "x-sharechat-userid": USER_ID,
            },
        },
        "type_specific_request": {  # gets media & metadata by content type within tag (image/video/text)
            "body": {
                "bn": "broker3",
                "userId": USER_ID,
                "passCode": PASSCODE,
                "client": "web",
                "message": {
                    "tagHash": "{}".format(tag_hash),
                    "feed": True,
                    # "allowOffline": True,
                    "type": "{}".format(content_type),
                },
            },
            "api_url": "https://restapi1.sharechat.com/requestType88",
            "headers": {
                "content-type": "application/json",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                "x-sharechat-authorized-userid": USER_ID,
                "x-sharechat-secret": PASSCODE,
                "x-sharechat-userid": USER_ID,
            },
        },
        "fresh_posts_request": {  # gets media & metadata by timestamp ("fresh" content)
            "body": {
                "bn": "broker3",
                "userId": USER_ID,
                "passCode": PASSCODE,
                "client": "web",
                "message": {
                    "th": "{}".format(tag_hash),
                    "s": "{}".format(unix_timestamp),
                    "allowOffline": True,
                },
            },
            "api_url": "https://restapi1.sharechat.com/requestType25",
            "headers": {
                "content-type": "application/json",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                "x-sharechat-authorized-userid": USER_ID,
                "x-sharechat-secret": PASSCODE,
                "x-sharechat-userid": USER_ID,
            },
        },
        "virality_metrics_request": {  # gets current virality metrics for a post
            "body": {
                "bn": "broker3",
                "userId": USER_ID,
                "passCode": PASSCODE,
                "client": "web",
                "message": {
                    "key": "{}".format(post_key),
                    "ph": "{}".format(post_key),
                    "allowOffline": True,
                },
            },
            "api_url": "https://restapi1.sharechat.com/requestType45",
            "headers": {
                "content-type": "application/json",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                "x-sharechat-authorized-userid": USER_ID,
                "x-sharechat-secret": PASSCODE,
                "x-sharechat-userid": USER_ID,
            },
        },
        "bucket_data_request": {  # gets list of tag hashes in a bucket
            "body": {
                "bn": "broker3",
                "userId": USER_ID,
                "passCode": PASSCODE,
                "client": "web",
                "message": {
                    "key": "{}".format(bucket_id),
                    "bucketId": bucket_id,
                    "t": 3,
                },
            },
            "api_url": "https://apis.sharechat.com/requestType66",
            "headers": {
                "content-type": "application/json",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                "x-sharechat-authorized-userid": USER_ID,
                "x-sharechat-secret": PASSCODE,
                "x-sharechat-userid": USER_ID,
            },
        },
    }
    return requests_dict


# Gets tag hashes from content  bucket
def get_tag_hashes(USER_ID, PASSCODE, bucket_ids):
    tag_hashes = []
    for i in bucket_ids:
        try:
            requests_dict = generate_requests_dict(
                USER_ID,
                PASSCODE,
                tag_hash=None,
                content_type=None,
                unix_timestamp=None,
                post_key=None,
                bucket_id=i,
            )
            bucket_data_response_dict = get_response_dict(
                requests_dict=requests_dict, request_type="bucket_data_request"
            )
            for tag in bucket_data_response_dict["payload"]["tags"]:
                tag_hashes.append(tag["tagHash"])
        except Exception:
            print(logging.traceback.format_exc())
    return tag_hashes


def get_response_dict(requests_dict, request_type):
    url = requests_dict[request_type]["api_url"]
    headers = requests_dict[request_type]["headers"]
    if request_type == "tag_data_request":
        response = requests.get(url=url, headers=headers)
        response_dict = json.loads(response.text)
    else:
        body = requests_dict[request_type]["body"]
        response = requests.post(url=url, json=body, headers=headers)
        response_dict = json.loads(response.text)
    return response_dict


def get_tag_data(payload_dict):
    tag_name = payload_dict.get("tagName")
    tag_translation = payload_dict.get("englishMeaning")
    tag_genre = payload_dict.get("tagGenre")
    bucket_name = payload_dict.get("bucketName")
    bucket_id = payload_dict.get("bucketId")
    tag_id = payload_dict.get("tagHash")
    tag_category = payload_dict.get("tagCategory")
    tag_creation = payload_dict.get("groupTag").get("createdOn")
    tag_reports = payload_dict.get("groupTag").get("reportCount")
    tag_members = payload_dict.get("groupTag").get("totalMemberCount")
    tag_rejects = payload_dict.get("groupTag").get("rejectedPostCount")
    return (
        tag_name,
        tag_translation,
        tag_genre,
        bucket_name,
        bucket_id,
        tag_category,
        tag_creation,
        tag_id,
        tag_reports,
        tag_members,
        tag_rejects,
    )


# Gets payload metadata that is common across content types
def get_common_metadata(
    payload_key,
    timestamp,
    language,
    media_type,
    post_permalink,
    caption,
    external_shares,
    likes,
    comments,
    reposts,
    views,
    profile_page,
    verified,
):
    timestamp.append(payload_key.get("o"))
    language.append(payload_key.get("m"))
    media_type.append(payload_key.get("t"))
    post_permalink.append(payload_key.get("permalink"))
    profile_page.append(
        "https://sharechat.com/profile/" + payload_key.get("ath").get("h")
    )
    verified.append(int(payload_key.get("ath").get("vp")))
    # if "c" in payload_key.keys():
    caption.append(payload_key.get("c"))
    # else:
    #     caption.append(None)
    virality_metrics = {
        "usc": external_shares,
        "lc": likes,
        "c2": comments,
        "repostCount": reposts,
        "l": views,
    }
    for metric in virality_metrics:
        if metric in payload_key.keys():
            virality_metrics[metric].append(payload_key.get(metric))
        else:
            virality_metrics[metric].append(0)


# Gets tag contents i.e. metadata for each post
def get_post_data(
    payload_dict,
    tag_name,
    tag_translation,
    tag_genre,
    bucket_name,
    bucket_id,
    tag_category,
    tag_creation,
    tag_id,
    tag_reports,
    tag_members,
    tag_rejects,
):
    media_link = []
    timestamp = []
    language = []
    media_type = []
    external_shares = []
    likes = []
    comments = []
    reposts = []
    post_permalink = []
    caption = []
    text = []
    views = []
    profile_page = []
    verified = []

    for i in payload_dict.get("payload").get("d"):

        if "repostId" in i:
            i["t"] = "repost"
            x = ""
            if "x" in i:
                x = i.get("x")
            get_common_metadata(i, timestamp, language, media_type, post_permalink, caption, external_shares, likes,
                                comments, reposts, views, profile_page, verified)
            shared_post_url = ""
            if "repostData" in i:
                shared_post_url = "https://sharechat.com/post/" + i.get("repostData").get("ph")
            text.append({"text": x, "shared_post": shared_post_url})
            media_link.append(None)
        elif i.get("t") == "image":
            get_common_metadata(i, timestamp, language, media_type, post_permalink, caption, external_shares, likes, comments, reposts, views, profile_page, verified)
            media_link.append(i.get("g"))
            text.append(None)
        elif i.get("t") == "video":
            get_common_metadata(
                i,
                timestamp,
                language,
                media_type,
                post_permalink,
                caption,
                external_shares,
                likes,
                comments,
                reposts,
                views,
                profile_page,
                verified,
            )
            media_link.append(i.get("v"))
            text.append(None)
        elif i.get("t") == "text":
            if "x" in i.keys():  # if post metadata contains the text
                get_common_metadata(
                    i,
                    timestamp,
                    language,
                    media_type,
                    post_permalink,
                    caption,
                    external_shares,
                    likes,
                    comments,
                    reposts,
                    views,
                    profile_page,
                    verified,
                )
                text.append(i.get("x"))
                media_link.append(None)
            else:
                pass
        elif i.get("t") == "link":
            if "ld" in i.keys():  # if post metadata contains link description
                get_common_metadata(
                    i,
                    timestamp,
                    language,
                    media_type,
                    post_permalink,
                    caption,
                    external_shares,
                    likes,
                    comments,
                    reposts,
                    views,
                    profile_page,
                    verified,
                )
                media_link.append(i.get("hl"))
                text.append(i.get("ld"))
            else:
                pass
        else:
            pass

    post_data = pd.DataFrame(
        np.column_stack(
            [
                media_link,
                timestamp,
                language,
                media_type,
                external_shares,
                likes,
                comments,
                reposts,
                post_permalink,
                caption,
                text,
                views,
                profile_page,
                verified,
            ]
        ),
        columns=[
            "media_link",
            "timestamp",
            "language",
            "media_type",
            "external_shares",
            "likes",
            "comments",
            "reposts",
            "post_permalink",
            "caption",
            "text",
            "views",
            "profile_page",
            "verified",
        ],
    )
    post_data["tag_name"] = tag_name
    post_data["tag_translation"] = tag_translation
    post_data["tag_genre"] = tag_genre
    post_data["bucket_name"] = bucket_name
    post_data["bucket_id"] = bucket_id
    post_data["tag_category"] = tag_category
    post_data["tag_creation"] = tag_creation
    post_data["tag_id"] = tag_id
    post_data["tag_reports"] = tag_reports
    post_data["tag_members"] = tag_members
    post_data["tag_rejects"] = tag_rejects
    return post_data


# Gets next offset hash for scraping the next page
def get_next_offset_hash(payload_dict):
    if "nextOffsetHash" in payload_dict["payload"]:
        next_offset_hash = payload_dict["payload"]["nextOffsetHash"]
    else:
        next_offset_hash = None
    return next_offset_hash


# Gets next timestamp for scraping the next page
def get_next_timestamp(payload_dict):
    next_timestamp = payload_dict.get("payload").get("n")
    # if "n" in payload_dict["payload"]:
    #     next_timestamp = payload_dict["payload"]["n"]
    # else:
    #     next_timestamp=None
    return next_timestamp


# Gets trending tag data
def get_trending_data(USER_ID, PASSCODE, tag_hashes, pages, delay):
    # Create empty dataframe to collect scraped data
    df = pd.DataFrame(
        columns=[
            "media_link",
            "timestamp",
            "language",
            "media_type",
            "tag_name",
            "tag_translation",
            "tag_genre",
            "bucket_name",
            "bucket_id",
            "external_shares",
            "likes",
            "comments",
            "reposts",
            "post_permalink",
            "caption",
            "text",
            "views",
            "profile_page",
            "tag_category",
            "tag_creation",
            "tag_id",
            "tag_reports",
            "tag_members",
            "tag_rejects",
        ]
    )
    content_types = ["image", "video", "text"]  # add others if required
    for tag_hash in tag_hashes:
        # next_offset_hash = None
        next_offset_hash = "kdn0"
        tagDataScraped = False
        try:
            # Send API request to scrape tag info
            requests_dict = generate_requests_dict(
                USER_ID,
                PASSCODE,
                tag_hash=tag_hash,
                content_type=None,
                unix_timestamp=None,
                post_key=None,
            )
            requests_dict["tag_data_request"]["api_url"] = (
                requests_dict["tag_data_request"]["api_url"]
                + tag_hash
                + "&groupTag=true"
            )
            tag_data_response_dict = get_response_dict(
                requests_dict=requests_dict, request_type="tag_data_request"
            )
            (
                tag_name,
                tag_translation,
                tag_genre,
                bucket_name,
                bucket_id,
                tag_category,
                tag_creation,
                tag_id,
                tag_reports,
                tag_members,
                tag_rejects,
            ) = get_tag_data(tag_data_response_dict)
            tagDataScraped = True
        except Exception:
            print("Could not scrape data from '{}'".format(tag_hash))
            print("Continuing ...")
            pass
        # Send API requests to scrape tag media & metadata
        if tagDataScraped:
            # Scrape trending pages
            for i in range(pages):
                try:
                    if next_offset_hash is not None:
                        requests_dict["trending_posts_request"]["body"]["message"][
                            "nextOffsetHash"
                        ] = "{}".format(next_offset_hash)
                    else:
                        pass
                    post_data_response_dict = get_response_dict(
                        requests_dict=requests_dict,
                        request_type="trending_posts_request",
                    )
                    post_data = get_post_data(
                        post_data_response_dict,
                        tag_name,
                        tag_translation,
                        tag_genre,
                        bucket_name,
                        bucket_id,
                        tag_category,
                        tag_creation,
                        tag_id,
                        tag_reports,
                        tag_members,
                        tag_rejects,
                    )
                    next_offset_hash = get_next_offset_hash(post_data_response_dict)
                    df = df.append(post_data, sort=True)
                    time.sleep(delay)  # random time delay between requests
                except Exception:
                    print(logging.traceback.format_exc())

            # Scrape additional content by content type
            for c in content_types:
                try:
                    requests_dict["type_specific_request"]["body"]["message"][
                        "type"
                    ] = "{}".format(c)
                    type_specific_response_dict = get_response_dict(
                        requests_dict=requests_dict,
                        request_type="type_specific_request",
                    )
                    post_data = get_post_data(
                        type_specific_response_dict,
                        tag_name,
                        tag_translation,
                        tag_genre,
                        bucket_name,
                        bucket_id,
                        tag_category,
                        tag_creation,
                        tag_id,
                        tag_reports,
                        tag_members,
                        tag_rejects,
                    )
                    df = df.append(post_data, sort=True)
                    time.sleep(delay)
                except Exception:
                    print(logging.traceback.format_exc())
        else:
            pass
    df.drop_duplicates(subset=["post_permalink"], inplace=True)
    df["timestamp"] = df["timestamp"].apply(lambda x: datetime.utcfromtimestamp(int(x)))
    df["filename"] = [str(uuid.uuid4()) for x in range(len(df))]
    df["scraped_date"] = datetime.utcnow()
    df["scraper_type"] = "trending"
    return df


# Gets fresh tag data
def get_fresh_data(USER_ID, PASSCODE, tag_hashes, pages, unix_timestamp, delay):
    # Create empty dataframe to collect scraped data
    print("Getting fresh data ...")
    df = pd.DataFrame(
        columns=[
            "media_link",
            "timestamp",
            "language",
            "media_type",
            "tag_name",
            "tag_translation",
            "tag_genre",
            "bucket_name",
            "bucket_id",
            "external_shares",
            "likes",
            "comments",
            "reposts",
            "post_permalink",
            "caption",
            "text",
            "views",
            "profile_page",
            "tag_category",
            "tag_creation",
            "tag_id",
            "tag_reports",
            "tag_members",
            "tag_rejects",
        ]
    )
    for tag_hash in tag_hashes:
        request_timestamp = unix_timestamp
        tagDataScraped = False
        try:
            # Send API request to scrape tag info
            requests_dict = generate_requests_dict(
                USER_ID,
                PASSCODE,
                tag_hash=tag_hash,
                content_type=None,
                unix_timestamp=unix_timestamp,
                post_key=None,
            )
            requests_dict["tag_data_request"]["api_url"] = (
                requests_dict["tag_data_request"]["api_url"]
                + tag_hash
                + "&groupTag=true"
            )
            tag_data_response_dict = get_response_dict(
                requests_dict=requests_dict, request_type="tag_data_request"
            )
            (
                tag_name,
                tag_translation,
                tag_genre,
                bucket_name,
                bucket_id,
                tag_category,
                tag_creation,
                tag_id,
                tag_reports,
                tag_members,
                tag_rejects,
            ) = get_tag_data(tag_data_response_dict)
            tagDataScraped = True
        except Exception:
            print(logging.traceback.format_exc())
            print("Could not scrape data from '{}'".format(tag_hash))
            print("Continuing ...")
            pass
        # Send API requests to scrape tag media & metadata
        if tagDataScraped:
            # Scrape fresh pages
            for i in range(pages):
                try:
                    requests_dict["fresh_posts_request"]["body"]["message"][
                        "s"
                    ] = "{}".format(request_timestamp)
                    fresh_posts_response_dict = get_response_dict(
                        requests_dict=requests_dict, request_type="fresh_posts_request"
                    )
                    fresh_posts_data = get_post_data(
                        fresh_posts_response_dict,
                        tag_name,
                        tag_translation,
                        tag_genre,
                        bucket_name,
                        bucket_id,
                        tag_category,
                        tag_creation,
                        tag_id,
                        tag_reports,
                        tag_members,
                        tag_rejects,
                    )
                    request_timestamp = get_next_timestamp(fresh_posts_response_dict)
                    df = df.append(fresh_posts_data, sort=True)
                    time.sleep(delay)
                except Exception:
                    pass
        else:
            pass
    df.drop_duplicates(subset=["post_permalink"], inplace=True)
    df["timestamp"] = df["timestamp"].apply(lambda x: datetime.utcfromtimestamp(int(x)))
    df["filename"] = [str(uuid.uuid4()) for x in range(len(df))]
    df["scraped_date"] = datetime.utcnow()
    df["scraper_type"] = "fresh"
    return df


# Mongo upload function for targeted tag scraper
def sharechat_mongo_upload(df, coll):
    # coll = s3_mongo_helper.initialize_mongo()
    for i in df.to_dict("records"):
        s3_mongo_helper.upload_to_mongo(data=i, coll=coll)


# Generate html file with thumbnails for image and video posts
def get_thumbnails_from_s3(df):
    def path_to_image_html(path):
        return '<img src="' + path + '"width="200" >'

    thumbnail = []
    aws, bucket, s3 = s3_mongo_helper.initialize_s3()
    temp_dir = tempfile.mkdtemp(dir=os.getcwd())
    for link in df["s3_url"]:
        if link is not None:
            if link.split(".")[-1] == "mp4":
                video_input_path = link
                img_output_path = (
                    temp_dir.split("/")[-1]
                    + "/"
                    + link.split("/")[-1].split(".")[0]
                    + ".jpg"
                )
                filename = link.split("/")[-1].split(".")[0] + ".jpg"
                subprocess.call(
                    [
                        "ffmpeg",
                        "-i",
                        video_input_path,
                        "-ss",
                        "00:00:00.000",
                        "-vframes",
                        "1",
                        img_output_path,
                    ],
                    stderr=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                )
                s3_mongo_helper.upload_to_s3(
                    s3=s3,
                    file=img_output_path,
                    filename=filename,
                    bucket=bucket,
                    content_type="image/jpeg",
                )
                thumbnail.append(aws + bucket + "/" + filename)
            elif link.split(".")[-1] == "txt":
                thumbnail.append(None)
            else:  # if jpg/jpeg/png
                thumbnail.append(link)
        else:  # if NaN
            thumbnail.append(None)
    df["thumbnail"] = np.array(thumbnail)
    pd.set_option("display.max_colwidth", -1)
    df_html = HTML(
        df.to_html(
            index=False,
            escape=False,
            formatters=dict(thumbnail=path_to_image_html),
            render_links=True,
        )
    )
    shutil.rmtree(temp_dir)
    return df, df_html


def get_thumbnails_from_sharechat(df):
    def path_to_image_html(path):
        return '<img src="' + path + '"width="200" >'

    thumbnail = []
    temp_dir = tempfile.mkdtemp(dir=os.getcwd())
    for link in df["media_link"]:
        if link is not None and "sharechat" in link:
            if link.split(".")[-1] == "mp4":
                video_input_path = link
                img_output_path = (
                    temp_dir.split("/")[-1]
                    + "/"
                    + link.split("/")[-1].split(".")[0]
                    + ".jpg"
                )
                subprocess.call(
                    [
                        "ffmpeg",
                        "-i",
                        video_input_path,
                        "-ss",
                        "00:00:00.000",
                        "-vframes",
                        "1",
                        img_output_path,
                    ],
                    stderr=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                )
                thumbnail.append(img_output_path)
            elif link.split(".")[-1] == "txt":
                thumbnail.append(None)
            else:  # if jpg/jpeg/png
                thumbnail.append(link)
        else:  # if NaN
            thumbnail.append(None)
    df["thumbnail"] = np.array(thumbnail)
    # print(df["thumbnail"])
    pd.set_option("display.max_colwidth", -1)
    df_html = HTML(
        df.to_html(
            index=False,
            escape=False,
            formatters=dict(thumbnail=path_to_image_html),
            render_links=True,
        )
    )
    shutil.rmtree(temp_dir)
    return df, df_html


# Virality scraper helper functions
def save_updated_df(df, today):
    df.to_csv("virality_df_{}.csv".format(today), index=False)


def scrape_metrics(response_dict):
    virality_metrics = {
        "c2": "comments",
        "usc": "external_shares",
        "lc": "likes",
        "repostCount": "reposts",
        "l": "views",
    }
    values = []
    for key in virality_metrics:
        if key in response_dict.get("payload").get("d").keys():
            res = int(response_dict.get("payload").get("d")[key])
            values.append(res)
        else:
            values.append(0)
    return values


def get_current_metrics(USER_ID, PASSCODE, post_permalink):
    post_key = post_permalink.split("/")[-1]
    requests_dict = generate_requests_dict(
        USER_ID,
        PASSCODE,
        tag_hash=None,
        content_type=None,
        unix_timestamp=None,
        post_key=post_key,
    )
    # Send API request & get response
    virality_metrics_response_dict = get_response_dict(
        requests_dict=requests_dict, request_type="virality_metrics_request"
    )
    # Scrape current metrics for post
    result = scrape_metrics(virality_metrics_response_dict)
    time.sleep(uniform(3, 5))
    return result


# s3 upload functions for ML scraper


def ml_initialize_s3():
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY_ID")
    aws = os.environ.get("AWS_BASE_URL")
    bucket = os.environ.get("AWS_ML_BUCKET")  # changed bucket
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    return aws, bucket, s3


def ml_upload_to_s3(s3, file, filename, bucket, content_type):
    with open(file, "rb") as data:
        s3.upload_fileobj(
            Fileobj=data, Bucket=bucket, Key="machinelearning-negatives/" + filename
        )


def ml_sharechat_s3_upload(df, aws, bucket, s3):
    for index, row in df.iterrows():
        try:
            if row["media_type"] == "image":
                # Create S3 file name
                filename = row["filename"] + ".jpg"
                # Get media
                temp = wget.download(row["media_link"])
                # Upload media to S3
                ml_upload_to_s3(
                    s3=s3,
                    file=temp,
                    filename=filename,
                    bucket=bucket,
                    content_type=row["media_type"],
                )
                os.remove(temp)
            elif row["media_type"] == "video":
                # Create S3 file name
                filename = row["filename"] + ".mp4"
                # Get media
                temp = wget.download(row["media_link"])
                # Upload media to S3
                ml_upload_to_s3(
                    s3=s3,
                    file=temp,
                    filename=filename,
                    bucket=bucket,
                    content_type=row["media_type"],
                )
                os.remove(temp)
            else:  # for text posts and media links
                # Create S3 file name
                filename = row["filename"] + ".txt"
                # Create text file
                with open("temp.txt", "w+") as f:
                    f.write(row["text"])
                    # Upload media to S3
                ml_upload_to_s3(
                    s3=s3,
                    file="temp.txt",
                    filename=filename,
                    bucket=bucket,
                    content_type=row["media_type"],
                )
                os.remove("temp.txt")
        except Exception:
            print(logging.traceback.format_exc())
            pass
    # Add S3 urls with correct extensions
    df.reset_index(inplace=True)
    df.loc[df["media_type"] == "image", "s3_url"] = (
        aws + bucket + "/" + df["filename"] + ".jpg"
    )
    df.loc[df["media_type"] == "video", "s3_url"] = (
        aws + bucket + "/" + df["filename"] + ".mp4"
    )
    df.loc[df["media_type"] == "text", "s3_url"] = (
        aws + bucket + "/" + df["filename"] + ".txt"
    )
    df.loc[df["media_type"] == "link", "s3_url"] = (
        aws + bucket + "/" + df["filename"] + ".txt"
    )
    return df  # return df with s3 urls added


def sharechat_s3_upload(df, aws, bucket, s3, coll):
    df.reset_index(drop=True, inplace=True)
    duplicates = []
    tags = []
    for index, row in df.iterrows():
        post_permalink = row["post_permalink"]
        # Check if post exists in Mongo DB
        if coll.count_documents({"post_permalink": post_permalink}) == 0:
            try:
                if row["media_type"] == "image":
                    # Create S3 file name
                    filename = row["filename"] + ".jpg"
                    # Get media
                    temp = wget.download(row["media_link"])
                    # Upload media to S3
                    s3_mongo_helper.upload_to_s3(
                        s3=s3,
                        file=temp,
                        filename=filename,
                        bucket=bucket,
                        content_type="image/jpeg",
                    )
                    os.remove(temp)
                elif row["media_type"] == "video":
                    # Create S3 file name
                    filename = row["filename"] + ".mp4"
                    # Get media
                    temp = wget.download(row["media_link"])
                    # Upload media to S3
                    s3_mongo_helper.upload_to_s3(
                        s3=s3,
                        file=temp,
                        filename=filename,
                        bucket=bucket,
                        content_type="video/mp4",
                    )
                    os.remove(temp)

                elif (row["media_type"] == "repost"):
                    filename = row["filename"]+".txt"
                    with codecs.getwriter("utf8")(open("temp.txt", "wb")) as f:
                        f.write(str(row["text"]))
                    s3_mongo_helper.upload_to_s3(s3=s3, file="temp.txt", filename=filename, bucket=bucket, content_type="application/json")
                    os.remove("temp.txt")
                else: # for text posts and media links
                        # Create S3 file name
                    filename = row["filename"]+".txt"
                        # Create text file
                      
                    with codecs.getwriter("utf8")(open("temp.txt", "wb")) as f:
                        f.write(row["text"])
                    # with open("temp.txt", "w+") as f:
                    #     f.write(row["text"])
                    # Upload media to S3
                    s3_mongo_helper.upload_to_s3(
                        s3=s3,
                        file="temp.txt",
                        filename=filename,
                        bucket=bucket,
                        content_type="application/json",
                    )
                    os.remove("temp.txt")
            except Exception:
                pass
        else:
            duplicates.append(index)
            tags.append(row["tag_name"])
    # Drop duplicates after saving tagwise duplicate count
    print(" ")
    print(
        "{} out of {} scraped posts already exist in database".format(
            len(duplicates), len(df)
        )
    )
    tagwise_duplicates = dict(zip(Counter(tags).keys(), Counter(tags).values()))
    df.drop(duplicates, axis=0, inplace=True)
    # Add S3 urls with correct extensions

    df.reset_index(drop=True, inplace = True)
    df.loc[df["media_type"] == "image", "s3_url"] = aws+bucket+"/"+df["filename"]+".jpg"
    df.loc[df["media_type"] == "video", "s3_url"] = aws+bucket+"/"+df["filename"]+".mp4"
    df.loc[df["media_type"] == "text", "s3_url"] = aws+bucket+"/"+df["filename"]+".txt"
    df.loc[df["media_type"] == "link", "s3_url"] = aws+bucket+"/"+df["filename"]+".txt"
    df.loc[df["media_type"] == "repost", "s3_url"] = aws+bucket+"/"+df["filename"]+".txt"
    return df, tagwise_duplicates # return df with s3 urls added

def ml_initialize_mongo():
    mongo_url = (
        "mongodb+srv://"
        + os.environ.get("SHARECHAT_DB_USERNAME")
        + ":"
        + os.environ.get("SHARECHAT_DB_PASSWORD")
        + "@tattle-data-fkpmg.mongodb.net/test?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE"
    )
    cli = MongoClient(mongo_url)
    db = cli[os.environ.get("SHARECHAT_DB_NAME")]
    coll = db[os.environ.get("SHARECHAT_ML_DB_COLLECTION")]
    if coll.count_documents({}) > 0:
        return coll
    else:
        print("Error accessing Mongo collection")
        sys.exit()


def ml_sharechat_mongo_upload(df, coll):
    # coll = s3_mongo_helper.initialize_mongo()
    for i in df.to_dict("records"):
        s3_mongo_helper.upload_to_mongo(data=i, coll=coll)


# S3 upload for logs


def initialize_s3_logbucket():
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY_ID")
    aws = os.environ.get("AWS_BASE_URL")
    bucket = os.environ.get("AWS_LOGS_BUCKET")  # changed bucket
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    return aws, bucket, s3


def upload_logs(s3, filename, key, bucket):
    s3.upload_file(
        Filename=filename, Bucket=bucket, Key="sharechat-scraper-logs/" + key
    )


# Old helper functions
# Saves data locally in csv and html formats
def save_data_to_disk(df, html):
    with open("sharechat_data_preview.html", "w") as f:
        f.write(html.data)
    df.drop("thumbnail", axis=1, inplace=True)
    df.to_csv("sharechat_data.csv")


# Converts links to thumbnails in html
def convert_links_to_thumbnails(df):
    df["thumbnail"] = df["media_link"]

    def path_to_image_html(path):
        return '<img src="' + path + '"width="200" >'

    image_df = df[df["media_type"] == "image"]
    pd.set_option("display.max_colwidth", -1)
    data_html = HTML(
        image_df.to_html(
            index=False,
            escape=False,
            formatters=dict(thumbnail=path_to_image_html),
            render_links=True,
        )
    )
    return data_html
