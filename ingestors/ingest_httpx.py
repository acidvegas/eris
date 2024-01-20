#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)

# HTTPX Log File Ingestion:
#
# This script takes JSON formatted HTTPX logs and indexes them into Elasticsearch.
#
# Saving my "typical" HTTPX command here for reference to myself:
#   httpx -l zone.org.txt -t 200 -r nameservers.txt -sc -location -favicon -title -bp -td -ip -cname -mc 200 -stream -sd -j -o output.json -v

import argparse
import json
import logging
import os

try:
    from elasticsearch import Elasticsearch, helpers
except ImportError:
    raise ImportError('Missing required \'elasticsearch\' library. (pip install elasticsearch)')


# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d %I:%M:%S')


class ElasticIndexer:
    def __init__(self, es_host: str, es_port: str, es_user: str, es_password: str, es_api_key: str, es_index: str, dry_run: bool = False, self_signed: bool = False):
        '''
        Initialize the Elastic Search indexer.

        :param es_host: Elasticsearch host
        :param es_port: Elasticsearch port
        :param es_user: Elasticsearch username
        :param es_password: Elasticsearch password
        :param es_api_key: Elasticsearch API Key
        :param es_index: Elasticsearch index name
        :param dry_run: If True, do not initialize Elasticsearch client
        :param self_signed: If True, do not verify SSL certificates
        '''

        self.dry_run = dry_run
        self.es = None
        self.es_index = es_index

        if not dry_run:

            if es_api_key:
                self.es = Elasticsearch([f'{es_host}:{es_port}'], headers={'Authorization': f'ApiKey {es_api_key}'}, verify_certs=self_signed, ssl_show_warn=self_signed)
            else:
                self.es = Elasticsearch([f'{es_host}:{es_port}'], basic_auth=(es_user, es_password), verify_certs=self_signed, ssl_show_warn=self_signed)


    def process_file(self, file_path: str, batch_size: int):
        '''
        Read and index HTTPX records in batches to Elasticsearch, handling large volumes efficiently.

        :param file_path: Path to the HTTPX log file
        :param batch_size: Number of records to process before indexing

        Example record:
        {
            "timestamp":"2024-01-14T13:08:15.117348474-05:00", # Rename to seen and remove milliseconds and offset
            "hash": {
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
            "url":"https://supernets.org",
            "input":"supernets.org", # rename to domain
            "title":"SuperNETs",
            "scheme":"https",
            "webserver":"nginx",
            "body_preview":"SUPERNETS Home About Contact Donate Docs Network IRC Git Invidious Jitsi LibreX Mastodon Matrix Sup",
            "content_type":"text/html",
            "method":"GET",
            "host":"51.89.151.158",
            "path":"/",
            "favicon":"-674048714",
            "favicon_path":"/i/favicon.png",
            "time":"592.907689ms",
            "a":[
                "51.89.151.158",
                "2001:41d0:801:2000::5ce9"
            ],
            "tech":[
                "Bootstrap:4.0.0",
                "HSTS",
                "Nginx"
            ],
            "words":436,
            "lines":79,
            "status_code":200,
            "content_length":4597,
            "failed":false,
            "knowledgebase":{
                "PageType":"nonerror",
                "pHash":0
            }
        }
        '''

        records = []

        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip()

                if not line:
                    continue

                record = json.loads(line)

                record['seen'] = record.pop('timestamp').split('.')[0] + 'Z' # Hacky solution to maintain ISO 8601 format without milliseconds or offsets
                record['domain'] = record.pop('input')

                del record['failed'], record['knowledgebase'], record['time']

                if self.dry_run:
                    print(record)
                else:
                    record = {'_index': self.es_index, '_source': record}
                    records.append(record)

                    if len(records) >= batch_size:
                        success, _ = helpers.bulk(self.es, records)
                        logging.info(f'Successfully indexed {success} records to {self.es_index}')
                        records = []

        if records:
            success, _ = helpers.bulk(self.es, records)
            logging.info(f'Successfully indexed {success} records to {self.es_index}')


def main():
    '''Main function when running this script directly.'''

    parser = argparse.ArgumentParser(description='Index data into Elasticsearch.')
    parser.add_argument('input_path', help='Path to the input file or directory')

    # General arguments
    parser.add_argument('--dry-run', action='store_true', help='Dry run (do not index records to Elasticsearch)')
    parser.add_argument('--batch_size', type=int, default=50000, help='Number of records to index in a batch')

    # Elasticsearch connection arguments
    parser.add_argument('--host', default='localhost', help='Elasticsearch host')
    parser.add_argument('--port', type=int, default=9200, help='Elasticsearch port')
    parser.add_argument('--user', default='elastic', help='Elasticsearch username')
    parser.add_argument('--password', default=os.getenv('ES_PASSWORD'), help='Elasticsearch password (if not provided, check environment variable ES_PASSWORD)')
    parser.add_argument('--api-key', help='Elasticsearch API Key for authentication')    
    parser.add_argument('--self-signed', action='store_false', help='Elasticsearch is using self-signed certificates')

    # Elasticsearch indexing arguments
    parser.add_argument('--index', default='zone-files', help='Elasticsearch index name')
    parser.add_argument('--shards', type=int, default=1, help='Number of shards for the index')
    parser.add_argument('--replicas', type=int, default=1, help='Number of replicas for the index')

    args = parser.parse_args()

    if not os.path.exists(args.input_path):
        raise FileNotFoundError(f'Input file {args.input_path} does not exist')

    if not args.dry_run:
        if args.batch_size < 1:
            raise ValueError('Batch size must be greater than 0')

        if not args.host:
            raise ValueError('Missing required Elasticsearch argument: host')
        
        if not args.api_key and (not args.user or not args.password):
            raise ValueError('Missing required Elasticsearch argument: either user and password or apikey')

    edx = ElasticIndexer(args.host, args.port, args.user, args.password, args.api_key, args.index, args.dry_run, args.self_signed)

    if os.path.isfile(args.input_path):
        logging.info(f'Processing file: {args.input_path}')
        edx.process_file(args.input_path, args.batch_size)

    elif os.path.isdir(args.input_path):
        logging.info(f'Processing files in directory: {args.input_path}')
        for file in sorted(os.listdir(args.input_path)):
            file_path = os.path.join(args.input_path, file)
            if os.path.isfile(file_path):
                logging.info(f'Processing file: {file_path}')
                edx.process_file(file_path, args.batch_size)

    else:
        raise ValueError(f'Input path {args.input_path} is not a file or directory')



if __name__ == '__main__':
    main()