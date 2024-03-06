#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_httpx.py

import json

default_index = 'httpx-logs'

def construct_map() -> dict:
    '''Construct the Elasticsearch index mapping for Masscan records.'''

    keyword_mapping = { 'type': 'text',  'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

    mapping = {
        'mappings': {
            'properties': {
                'change': 'me'
            }
        }
    }

    return mapping


def process_file(file_path: str):
    '''
    Read and process HTTPX records from the log file.

    :param file_path: Path to the HTTPX log file
    '''

    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()

            if not line:
                continue

            record = json.loads(line)

            record['seen'] = record.pop('timestamp').split('.')[0] + 'Z' # Hacky solution to maintain ISO 8601 format without milliseconds or offsets
            record['domain'] = record.pop('input')

            del record['failed'], record['knowledgebase'], record['time']

            yield record

    return None # EOF


  
''''
Example record:
{
    "timestamp":"2024-01-14T13:08:15.117348474-05:00", # Rename to seen and remove milliseconds and offset
    "hash": { # Do we need all of these ?
        "body_md5":"4ae9394eb98233b482508cbda3b33a66",
        "body_mmh3":"-4111954",
        "body_sha256":"89e06e8374353469c65adb227b158b265641b424fba7ddb2c67eef0c4c1280d3",
        "body_simhash":"9814303593401624250",
        "header_md5":"980366deb2b2fb5df2ad861fc63e79ce",
        "header_mmh3":"-813072798",
        "header_sha256":"39aea75ad548e38b635421861641ad1919ed3b103b17a33c41e7ad46516f736d",
        "header_simhash":"10962523587435277678"
    },
    "port":"443",
    "url":"https://supernets.org", # Remove this and only use the input field as "domain" maybe
    "input":"supernets.org", # rename to domain
    "title":"SuperNETs",
    "scheme":"https",
    "webserver":"nginx",
    "body_preview":"SUPERNETS Home About Contact Donate Docs Network IRC Git Invidious Jitsi LibreX Mastodon Matrix Sup",
    "content_type":"text/html",
    "method":"GET", # Do we need this ?
    "host":"51.89.151.158",
    "path":"/",
    "favicon":"-674048714",
    "favicon_path":"/i/favicon.png",
    "time":"592.907689ms", # Do we need this ?
    "a":[
        "6.150.220.23"
    ],
    "tech":[
        "Bootstrap:4.0.0",
        "HSTS",
        "Nginx"
    ],
    "words":436, # Do we need this ?
    "lines":79, # Do we need this ?
    "status_code":200, 
    "content_length":4597,
    "failed":false, # Do we need this ?
    "knowledgebase":{ # Do we need this ?
        "PageType":"nonerror",
        "pHash":0
    }
}
'''