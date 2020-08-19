# Import libraries
import os
import requests
import pandas as pd
import numpy as np
import re
import datetime
from datetime import datetime, date
from IPython.display import Image, HTML
import time
from time import sleep
from random import uniform
import json
import urllib
import uuid
import boto
import boto3
from boto3 import client
from PIL import Image
import io
from dotenv import load_dotenv
load_dotenv() 
import pymongo
from pymongo import MongoClient
import wget
import sharechat_helper
import s3_mongo_helper 
import tempfile
from tempfile import mkdtemp
import shutil
import subprocess
import logging
import pickle
from tqdm import tqdm
from datetime import timedelta

# Trending content scraper
def trending_content_scraper(USER_ID=None, PASSCODE=None, tag_hashes=None, bucket_ids=None, pages=None, mode=None, targeting=None):
    if targeting == "bucket":
        tag_hashes = sharechat_helper.get_tag_hashes(USER_ID, PASSCODE, bucket_ids)
        delay = uniform(10,15)
    elif targeting == "tag":
        delay = uniform(30,35)
    if mode == "archive":
        print("Scraping in archive mode")
        start_time = time.time()
        # Initialize S3 and Mongo DB 
        print("Initializing ...")
        initializationSuccess = False
        try:
            aws, bucket, s3 = s3_mongo_helper.initialize_s3()
            coll = s3_mongo_helper.initialize_mongo()
            initializationSuccess = True
            print("Initialized successfully")
        except Exception as e:
            print("Initialization failure")
            print(logging.traceback.format_exc())
        # Scrape data from Sharechat tags
        if initializationSuccess:
            print("Scraping in progress ...")
            sharechat_df = sharechat_helper.get_trending_data(
                                                    USER_ID,
                                                    PASSCODE,
                                                    tag_hashes,
                                                    pages,
                                                    delay)
            
            
            if len(sharechat_df) < 1: 
                raise ValueError("Returned empty dataframe. No posts were scraped.")
            else:
                # Save data to S3 & Mongo 
                s3UploadSuccess = False
                try:
                    print("S3 upload in progress ...")
                    sharechat_df, tagwise_duplicates = sharechat_helper.sharechat_s3_upload(sharechat_df, aws, bucket, s3, coll) # the returned df includes s3 urls
                    s3UploadSuccess = True
                    print("Data uploaded to S3")
                except Exception as e:
                    print("S3 upload failed")
                    print(logging.traceback.format_exc())
                    pass
                if s3UploadSuccess:
                    aws, logbucket, s3 = sharechat_helper.initialize_s3_logbucket()
                    today = datetime.utcnow().strftime("%Y%m%d")
                    try: 
                        print("HTML file creation in progress ...")
                        sharechat_df, sharechat_df_html = sharechat_helper.get_thumbnails_from_s3(sharechat_df)
                        with open("sharechat_trending_data_preview.html", "w") as f:
                            f.write(sharechat_df_html.data)
                            print("HTML file created")
                        print("Uploading HTML file to S3 ...")
                        sharechat_helper.upload_logs(s3=s3, filename="sharechat_trending_data_preview.html", key="trending_preview_"+today, bucket=logbucket)
                        print("HTML file uploaded")
                    except Exception as e:
                        print("HTML file upload failed")
                        print(logging.traceback.format_exc())
                        pass
                    try:
                        print("Duplicates log creation in progress ...")
                        with open('tagwise_duplicates.json', 'w') as fp:
                            json.dump(tagwise_duplicates, fp)
                        print("Duplicates log created")
                        print("Uploading duplicates log to S3 ...")
                        sharechat_helper.upload_logs(s3=s3, filename="tagwise_duplicates.json", key="trending_duplicates_"+today, bucket=logbucket)
                        print("Duplicates log uploaded")
                    except Exception as e:
                        print("Duplicates log upload failed")
                        print(logging.traceback.format_exc())
                        pass
                    try:
                        print("CSV file creation in progress ... ")
                        sharechat_df.to_csv("sharechat_trending_data.csv")
                        print("CSV file created")
                        print("Uploading CSV file to S3 ...")
                        sharechat_helper.upload_logs(s3=s3, filename="sharechat_trending_data.csv", key="trending_posts_"+today, bucket=logbucket)
                        print("CSV file uploaded")
                    except Exception as e:
                        print("CSV file upload failed")
                        print(logging.traceback.format_exc())
                        pass  
                    try:
                        print("MongoDB upload in progress ...")
                        sharechat_helper.sharechat_mongo_upload(sharechat_df, coll)
                        print("Data uploaded to MongoDB")
                        print("{} posts saved".format(len(sharechat_df)))            
                    except Exception as e:
                        print("MongoDB upload failed")
                        print(logging.traceback.format_exc())
                        pass
                else:
                    pass   
                print("Scraping complete")
                print("Time taken: %s seconds" % (time.time() - start_time))
                return sharechat_df
    elif mode == "local":
        print("Scraping in local mode")
        start_time = time.time()
        print("Scraping in progress ...")
        sharechat_df = sharechat_helper.get_trending_data(
                                                    USER_ID,
                                                    PASSCODE,
                                                    tag_hashes,
                                                    pages,
                                                    delay)
        if len(sharechat_df) < 1: 
            raise ValueError("Returned empty dataframe. No posts were scraped.")
        else:
            # Save data locally
            sharechat_df.to_pickle("sharechat_df.pkl")
        try: 
            print("HTML preview file creation in progress ...")
            sharechat_df, sharechat_df_html = sharechat_helper.get_thumbnails_from_sharechat(sharechat_df)
            with open("sharechat_trending_data_preview.html", "w") as f:
                f.write(sharechat_df_html.data)
                print("HTML preview file created")
        except Exception as e:
            print("HTML preview file creation failed")
            print(logging.traceback.format_exc())
            pass 
        try:
            print("CSV file creation in progress ... ")
            sharechat_df.to_csv("sharechat_trending_data.csv")
            print("CSV file created")
            print("{} posts saved".format(len(sharechat_df)))
        except Exception as e:
            print("CSV file creation failed")
            print(logging.traceback.format_exc())
            pass
        print("Scraping complete")
        print("Time taken: %s seconds" % (time.time() - start_time))
        return sharechat_df



# Fresh content scraper
def fresh_content_scraper(USER_ID=None, PASSCODE=None, tag_hashes=None, bucket_ids=None, pages=None, unix_timestamp=None, mode=None, targeting=None):
    if targeting == "bucket":
        tag_hashes = sharechat_helper.get_tag_hashes(USER_ID, PASSCODE, bucket_ids)
        delay = uniform(10,15)
    elif targeting == "tag":
        delay = uniform(30,35)
    if mode == "archive":
        print("Scraping in archive mode")
        start_time = time.time()
        # Initialize S3 and Mongo DB 
        print("Initializing ...")
        initializationSuccess = False
        try:
            aws, bucket, s3 = s3_mongo_helper.initialize_s3()
            coll = s3_mongo_helper.initialize_mongo()
            initializationSuccess = True
            print("Initialized successfully")
        except Exception as e:
            print("Initialization failure")
            print(logging.traceback.format_exc())
        # Scrape data from Sharechat tags
        if initializationSuccess:
            print("Scraping in progress ...")
            sharechat_df = sharechat_helper.get_fresh_data(
                                                    USER_ID,
                                                    PASSCODE,
                                                    tag_hashes,
                                                    pages,
                                                    unix_timestamp,
                                                    delay)
        if len(sharechat_df) < 1:          
            raise ValueError("Returned empty dataframe. No posts were scraped.")
        else:
            # Save data to S3 & Mongo DB
            s3UploadSuccess = False
            try:
                print("S3 upload in progress ...")
                sharechat_df, tagwise_duplicates = sharechat_helper.sharechat_s3_upload(sharechat_df, aws, bucket, s3, coll) # the returned df includes s3 urls
                s3UploadSuccess = True
                print("Data uploaded to S3")
            except Exception as e:
                print("S3 upload failed")
                print(logging.traceback.format_exc())
                pass
            if s3UploadSuccess:
                aws, logbucket, s3 = sharechat_helper.initialize_s3_logbucket()
                today = datetime.utcnow().strftime("%Y%m%d")
                try: 
                    print("HTML file creation in progress ...")
                    sharechat_df, sharechat_df_html = sharechat_helper.get_thumbnails_from_s3(sharechat_df)
                    with open("sharechat_fresh_data_preview.html", "w") as f:
                        f.write(sharechat_df_html.data)
                        print("HTML file created")
                    print("Uploading HTML file to S3 ...")
                    sharechat_helper.upload_logs(s3=s3, filename="sharechat_fresh_data_preview.html", key="fresh_preview_"+today, bucket=logbucket)
                    print("HTML file uploaded")
                except Exception as e:
                    print("HTML file upload failed")
                    print(logging.traceback.format_exc())
                    pass
                try:
                    print("Duplicates log creation in progress ...")
                    with open('tagwise_duplicates.json', 'w') as fp:
                        json.dump(tagwise_duplicates, fp)
                    print("Duplicates log created")
                    print("Uploading duplicates log to S3 ...")
                    sharechat_helper.upload_logs(s3=s3, filename="tagwise_duplicates.json", key="fresh_duplicates_"+today, bucket=logbucket)
                    print("Duplicates log uploaded")
                except Exception as e:
                    print("Duplicates log upload failed")
                    print(logging.traceback.format_exc())
                    pass
                try:
                    print("CSV file creation in progress ... ")
                    sharechat_df.to_csv("sharechat_fresh_data.csv")
                    print("CSV file created")
                    print("Uploading CSV file to S3 ...")
                    sharechat_helper.upload_logs(s3=s3, filename="sharechat_fresh_data.csv", key="fresh_posts_"+today, bucket=logbucket)
                    print("CSV file uploaded")
                except Exception as e:
                    print("CSV file upload failed")
                    print(logging.traceback.format_exc())
                    pass  
                try:
                    print("MongoDB upload in progress ...")
                    sharechat_helper.sharechat_mongo_upload(sharechat_df, coll)
                    print("Data uploaded to MongoDB")
                    print("{} posts saved".format(len(sharechat_df)))            
                except Exception as e:
                    print("MongoDB upload failed")
                    print(logging.traceback.format_exc())
                    pass
            else:
                pass   
            print("Scraping complete")
            print("Time taken: %s seconds" % (time.time() - start_time))
            return sharechat_df
    elif mode == "local":
        print("Scraping in local mode")
        start_time = time.time()
        print("Scraping in progress ...")
        sharechat_df = sharechat_helper.get_fresh_data(
                                                    USER_ID,
                                                    PASSCODE,
                                                    tag_hashes,
                                                    pages,
                                                    unix_timestamp,
                                                    delay)
        if len(sharechat_df) < 1:          
            raise ValueError("Returned empty dataframe. No posts were scraped.")
        else:
            # Save data locally
            sharechat_df.to_pickle("sharechat_df.pkl")
        try: 
            print("HTML preview file creation in progress ...")
            sharechat_df, sharechat_df_html = sharechat_helper.get_thumbnails_from_sharechat(sharechat_df)
            with open("sharechat_fresh_data_preview.html", "w") as f:
                f.write(sharechat_df_html.data)
                print("HTML preview file created")
        except Exception as e:
            print("HTML preview file creation failed")
            print(logging.traceback.format_exc())
            pass 
        try:
            print("CSV file creation in progress ... ")
            sharechat_df.to_csv("sharechat_fresh_data.csv")
            print("CSV file created")
            print("{} posts saved".format(len(sharechat_df)))
        except Exception as e:
            print("CSV file creation failed")
            print(logging.traceback.format_exc())
            pass
        print("Scraping complete")
        print("Time taken: %s seconds" % (time.time() - start_time))
        return sharechat_df



# ML scraper (modified version of trending content scraper)
def ml_scraper(USER_ID=None, PASSCODE=None, tag_hashes=None, bucket_ids=None, pages=None, mode=None, targeting=None):
    if targeting == "bucket":
        tag_hashes = sharechat_helper.get_tag_hashes(USER_ID, PASSCODE, bucket_ids)
        delay = uniform(10,15)
    elif targeting == "tag":
        delay = uniform(30,35)
    if mode == "archive":
        print("Scraping in archive mode")
        start_time = time.time()
        print("Initializing ...")
        initializationSuccess = False
        try:
            coll = sharechat_helper.ml_initialize_mongo()
            aws, bucket, s3 = sharechat_helper.ml_initialize_s3()
            initializationSuccess = True
            print("Initialized successfully")
        except Exception as e:
            print("Initialization failure")
            print(logging.traceback.format_exc())
        # Scrape data from tags
        if initializationSuccess:
            print("Scraping in progress ...")
            sharechat_df = sharechat_helper.get_trending_data(
                                                    USER_ID,
                                                    PASSCODE,
                                                    tag_hashes,
                                                    pages,
                                                    delay)
            if len(sharechat_df) < 1: 
                raise ValueError("Returned empty dataframe. No posts were scraped.")
            else:
                # Save data locally
                sharechat_df.to_pickle("sharechat_df.pkl")
                s3UploadSuccess = False
                # Save data to S3 & Mongo DB
                try:
                    print("S3 upload in progress ... ")
                    sharechat_df = sharechat_helper.ml_sharechat_s3_upload(sharechat_df, aws, bucket, s3) 
                    s3UploadSuccess = True
                    print("Data uploaded to S3")
                except Exception as e:
                    print("S3 upload failed")
                    print(logging.traceback.format_exc())
                    pass
                if s3UploadSuccess:
                    try: 
                        print("HTML preview file creation in progress ...")
                        sharechat_df, sharechat_df_html = sharechat_helper.get_thumbnails_from_s3(sharechat_df)
                        with open("sharechat_ml_data_preview.html", "w") as f:
                            f.write(sharechat_df_html.data)
                            print("HTML preview file created")
                    except Exception as e:
                        print("HTML preview file creation failed")
                        print(logging.traceback.format_exc())
                        pass 
                    try:
                        print("MongoDB upload in progress ...")
                        sharechat_helper.sharechat_mongo_upload(sharechat_df, coll)
                        print("Data uploaded to MongoDB")            
                    except Exception as e:
                        print("MongoDB upload failed")
                        print(logging.traceback.format_exc())
                        pass  
                else:
                    pass
                try:
                    print("CSV file creation in progress ... ")
                    sharechat_df.to_csv("sharechat_ml_data.csv")
                    print("CSV file created")
                    print("{} posts scraped".format(len(sharechat_df)))
                except Exception as e:
                    print("CSV file creation failed")
                    print(logging.traceback.format_exc())
                    pass
                print("Scraping complete")
                print("Time taken: %s seconds" % (time.time() - start_time))
                return sharechat_df
    elif mode == "local":
        print("Scraping in local mode")
        start_time = time.time()
        print("Scraping in progress ...")
        sharechat_df = sharechat_helper.get_trending_data(
                                                USER_ID,
                                                PASSCODE,
                                                tag_hashes,
                                                pages,
                                                delay)
        if len(sharechat_df) < 1: 
            raise ValueError("Returned empty dataframe. No posts were scraped.")
        else:
            # Save data locally
            sharechat_df.to_pickle("sharechat_df.pkl")
        try: 
            print("HTML preview file creation in progress ...")
            sharechat_df, sharechat_df_html = sharechat_helper.get_thumbnails_from_sharechat(sharechat_df)
            with open("sharechat_ml_data_preview.html", "w") as f:
                f.write(sharechat_df_html.data)
                print("HTML preview file created")
        except Exception as e:
            print("HTML preview file creation failed")
            print(logging.traceback.format_exc())
            pass 
        try:
            print("CSV file creation in progress ... ")
            sharechat_df.to_csv("sharechat_ml_data.csv")
            print("CSV file created")
            print("{} posts scraped".format(len(sharechat_df)))
        except Exception as e:
            print("CSV file creation failed")
            print(logging.traceback.format_exc())
            pass
        print("Scraping complete")
        print("Time taken: %s seconds" % (time.time() - start_time))
        return sharechat_df
    
# Virality metrics scraper
def virality_scraper(USER_ID=None, PASSCODE=None, virality_job=None):

    start_time = time.time()
    # Initialize S3 and Mongo DB 
    print("Initializing Mongo DB ...")
    initializationSuccess = False
    try:
        coll = s3_mongo_helper.initialize_mongo()
        initializationSuccess = True
        print("Initialized successfully")
    except Exception as e:
        print("Initialization failure")
        print(logging.traceback.format_exc())

    if initializationSuccess:
        updates=0
        failed=0
        today = datetime.utcnow() 

        if virality_job == 1:     
            # Get metrics for t+1 & t+2
            start = today - timedelta(days=2)
            end = today - timedelta(days=1)
            print("# Updating virality metrics for posts 1 & 2 day old posts ...")
        
        elif virality_job == 2:     
            # Get metrics for t+3 ... t+5
            start = today - timedelta(days=5)
            end = today - timedelta(days=3)
            print("# Updating virality metrics for 3, 4 & 5 day old posts ...")
        
        cursor = coll.find({"scraped_date": {"$gte": start, "$lte": end}, "scraper_type": "fresh"})
        for doc in cursor:
            try:
                # Get timestamp for day t
                timestamp = pd.to_datetime(doc["timestamp"])
                # Calculate days since t
                diff = str((today-timestamp).days)
                # Get current virality metrics
                result = sharechat_helper.get_current_metrics(USER_ID, PASSCODE, doc["post_permalink"])
                # Update doc
                coll.update({"_id": doc["_id"]},
                            {"$set": {
                                "comments_t+"+diff: result[0],
                                "external_shares_t+"+diff: result[1],
                                "likes_t+"+diff: result[2],
                                "reposts_t+"+diff: result[3],
                                "views_t+"+diff: result[4]
                                }
                            }
                        )
                updates+=1

                # For debugging
                # print(coll.find_one({"_id": doc["_id"]}))
                # print("")

            except Exception as e:
                failed+=1
                pass

        print("Scraping complete")
        print("Updated virality metrics for {} posts".format(updates))
        print("{} updates failed".format(failed))
        print("Time taken: %s seconds" % (time.time() - start_time))


    
    