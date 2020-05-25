#!/usr/bin/env python

import argparse
import getpass
import json
import logging
import os
import requests
import sys

import qserv.ingest as ingest

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A thin python wrapper around Qserv REST web service")

    parser.add_argument("url", type=str, help="Web Service URL")
    parser.add_argument("method", type=str, help="Request type", choices=["post", "put"])
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--data", help="Key-value pairs to pass configs with the request",
                       metavar="KEY=VALUE", nargs="*", action=ingest.DataAction)
    group.add_argument("--json", type=str, help="Json file with the request configs", action=ingest.JsonAction)
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
        sys.exit(ingest.post(args.url, payload))
    elif args.method == "put":
        sys.exit(ingest.put(args.url, payload))
