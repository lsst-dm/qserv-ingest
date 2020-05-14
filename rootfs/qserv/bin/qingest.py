#!/usr/bin/env python

import argparse
import getpass
import json
import logging
import os
import requests
import sys

AUTH_PATH = "~/.lsst/qserv"


def authorize():
    try:
        with open(os.path.expanduser(AUTH_PATH), 'r') as f:
            authKey = f.read().strip()
    except:
        logging.debug("Cannot find %s", AUTH_PATH)
        authKey = getpass.getpass()
    return authKey


def put(url, payload=None):
    authKey = authorize()
    response = requests.put(url, json={"auth_key": authKey})
    responseJson = response.json()
    if not responseJson['success']:
        logging.critical("%s %s", url, responseJson['error'])
        return 1
    logging.debug(responseJson)
    logging.info("success")


def post(url, payload):
    authKey = authorize()
    payload["auth_key"] = authKey
    response = requests.post(url, json=payload)
    del payload["auth_key"]
    responseJson = response.json()
    if not responseJson["success"]:
        logging.critical(responseJson["error"])
        return 1
    logging.debug(responseJson)
    logging.debug("success")

    # For catching the super transaction ID
    # Want to print responseJson["databases"]["hsc_test_w_2020_14_00"]["transactions"]
    if "databases" in responseJson:
        # try:
        transId = list(responseJson["databases"].values())[0]["transactions"][0]["id"]
        logging.debug(f"transaction ID is {transId}")
        logging.info(transId)

    # For catching the location host and port
    if "location" in responseJson and "chunk" in payload:
        host = responseJson["location"]["host"]
        port = responseJson["location"]["port"]
        logging.info("%d %s %d" % (payload["chunk"], host, port))


class DataAction(argparse.Action):
    """argparse action to attempt casting the values to floats and put into a dict"""

    def __call__(self, parser, namespace, values, option_string):
        d = dict()
        for item in values:
            k, v = item.split("=")
            try:
                v = float(v)
            except ValueError:
                pass
            finally:
                d[k] = v
        setattr(namespace, self.dest, d)


class JsonAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        with open(values, 'r') as f:
            x = json.load(f)
        setattr(namespace, self.dest, x)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A thin python wrapper around Qserv REST web service")

    parser.add_argument("url", type=str, help="Web Service URL")
    parser.add_argument("method", type=str, help="Request type", choices=["post", "put"])
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--data", help="Key-value pairs to pass configs with the request",
                       metavar="KEY=VALUE", nargs="*", action=DataAction)
    group.add_argument("--json", type=str, help="Json file with the request configs", action=JsonAction)
    parser.add_argument("--verbose", "-v", action="store_true", help="Use debug logging")
    args = parser.parse_args()

    logger = logging.getLogger()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    payload = args.data or args.json or dict()
    logging.debug("Starting a request: %s with %s", args.url, payload)
    if args.method == "post":
        sys.exit(post(args.url, payload))
    elif args.method == "put":
        sys.exit(put(args.url, payload))
