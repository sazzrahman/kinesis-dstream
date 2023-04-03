import datetime as dt
import json
import math
import os
import requests
import time
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError
from dotenv import load_dotenv
import unittest
import logging
from logging.handlers import RotatingFileHandler
import sys
from threading import Thread
import multiprocessing
from queue import Queue

# define connections
load_dotenv()
request_session = requests.Session()


def log_configure(logger_name, log_file_path):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    # the format of the log message
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')

    # define a handler for each parallel process
    handler = RotatingFileHandler(
        log_file_path, mode='a', maxBytes=1000000)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger


logger = log_configure('engagments', 'logs/engagement-tasks.log')


def get_time_window(now: dt.datetime, lag) -> tuple:
    """
    lag is in seconds
    """
    utc_now_ms = math.floor(now.timestamp() * 1000)
    utc_before = now-dt.timedelta(seconds=lag)
    utc_before_ms = math.floor(utc_before.timestamp() * 1000)

    return utc_before_ms, utc_now_ms


def get_http_response(engagement_type, properties, now, limit, after, lag) -> dict:
    """
    sends GET request to the provided endpoint url
    """

    url = f"https://api.hubapi.com/crm/v3/objects/{engagement_type}/search?"
    token = os.getenv("HS_TOKEN")

    before_ms, now_ms = get_time_window(now=now, lag=lag)

    payload = json.dumps({
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "hs_lastmodifieddate",
                        "operator": "BETWEEN",
                        "highValue": f"{now_ms}",
                        "value": f"{before_ms}"
                    }
                ]
            }
        ],
        "properties": properties,
        "limit": limit,
        "after": after
    })
    headers = {
        'Authorization': f"Bearer {token}",
        'Content-Type': 'application/json'
    }

    response = request_session.request(
        "POST", url, headers=headers, data=payload)

    return response.status_code, response.json()


def get_data(properties, engagement_type, now, lag):
    iterate = True
    after = None
    while iterate:

        # fetch the key results
        try:
            # use current timestamp and lag
            status_code, data = get_http_response(now=now, properties=properties,
                                                  engagement_type=engagement_type, lag=lag, limit=100, after=after)
            # time stamp for the current interval
            utc_before = now-dt.timedelta(seconds=lag)
            logger.info(
                f"{engagement_type}::lag interval timestamps {utc_before} :: {now}")
        except Exception as e:
            print(e)
            logger.info("waiting few seconds before retrying")
            time.sleep(10)
            # must continue to retry the api call
            continue

        # if response has 200 status code and results
        if status_code == 200:
            after = data.get("paging").get("next").get(
                "after") if data.get("paging") else None

            # if there is data in the current interval then put it kinesis datastream
            if data.get('total') != 0:
                # put data in kinesis stream
                results = data.get("results")

                logger.info(f"{engagement_type}::{data}")

            if not after:
                iterate = False

        else:
            logger.error(data)
            if data.get("errorType") == 'RATE_LIMIT':
                time.sleep(10)
                # retry the api call with current parameters
                continue
            else:
                # raise exception if there is any other error
                logger.exception("UNPRECEDENTED ERROR :: ", data)
                raise Exception("Exception :: ", data)

    # recursively all the function with next timestamp and lag
    begin_time = now
    # wait for 10 seconds before calling the function again
    time.sleep(10)
    # the lag is the difference between the current time and the time when the function was called
    now = dt.datetime.now()
    lag = now - begin_time

    return get_data(properties, engagement_type, now, lag.seconds)


class TestStreamEngagements(unittest.TestCase):
    def test_load_variables(self):
        self.assertIsNotNone(os.getenv("HS_TOKEN"))
        self.assertIsNotNone(os.getenv("HS_TASK_PROPERTIES"))
        self.assertIsNotNone(os.getenv("KINESIS_STREAM_NAME"))


def poll_data(properties, engagement_type):
    now = dt.datetime.now()
    lag = 10
    get_data(properties, engagement_type, now, lag)


if __name__ == "__main__":
    engagements = ["tasks", "calls", "meetings", "emails"]
    # engagements = ["tasks", "calls", "meetings", "emails", "communications"]
    NUM_THREADS = len(engagements)

    for t in range(NUM_THREADS):

        current_property = os.getenv(
            f"HS_{engagements[t].upper()}_PROPERTIES")
        if not current_property:
            raise Exception(f"Property Not Found :: {current_property}")

        properties = current_property.split("|")
        # Thread(target=poll_data, args=(properties, engagements[t])).start()
        p = multiprocessing.Process(
            target=poll_data, args=(properties, engagements[t]))
        p.start()
