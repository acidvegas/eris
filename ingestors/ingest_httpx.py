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
import time

try:
    from elasticsearch import Elasticsearch, helpers
except ImportError:
    raise ImportError('Missing required \'elasticsearch\' library. (pip install elasticsearch)')


# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d %I:%M:%S')


class ElasticIndexer:
    def __init__(self, es_host: str, es_port: int, es_user: str, es_password: str, es_api_key: str, es_index: str, dry_run: bool = False, self_signed: bool = False, retries: int = 10, timeout: int = 30):
        '''
        Initialize the Elastic Search indexer.

        :param es_host: Elasticsearch host(s)
        :param es_port: Elasticsearch port
        :param es_user: Elasticsearch username
        :param es_password: Elasticsearch password
        :param es_api_key: Elasticsearch API Key
        :param es_index: Elasticsearch index name
        :param dry_run: If True, do not initialize Elasticsearch client
        :param self_signed: If True, do not verify SSL certificates
        :param retries: Number of times to retry indexing a batch before failing
        :param timeout: Number of seconds to wait before retrying a batch
        '''

        self.dry_run = dry_run
        self.es = None
        self.es_index = es_index

        if not dry_run:
            es_config = {
                'hosts': [f'{es_host}:{es_port}'],
                'verify_certs': self_signed,
                'ssl_show_warn': self_signed,
                'request_timeout': timeout,
                'max_retries': retries,
                'retry_on_timeout': True,
                'sniff_on_start': False,
                'sniff_on_node_failure': True,
                'min_delay_between_sniffing': 60
            }

            if es_api_key:
                es_config['headers'] = {'Authorization': f'ApiKey {es_api_key}'}
            else:
                es_config['basic_auth'] = (es_user, es_password)
                
            # Patching the Elasticsearch client to fix a bug with sniffing (https://github.com/elastic/elasticsearch-py/issues/2005#issuecomment-1645641960)
            import sniff_patch
            self.es = sniff_patch.init_elasticsearch(**es_config)

            # Remove the above and uncomment the below if the bug is fixed in the Elasticsearch client:
            #self.es = Elasticsearch(**es_config)


    def get_cluster_health(self) -> dict:
        '''Get the health of the Elasticsearch cluster.'''

        return self.es.cluster.health()
    

    def get_cluster_size(self) -> int:
        '''Get the number of nodes in the Elasticsearch cluster.'''

        cluster_stats = self.es.cluster.stats()
        number_of_nodes = cluster_stats['nodes']['count']['total']

        return number_of_nodes


    def process_file(self, file_path: str, watch: bool = False, chunk: dict = {}):
        '''
        Read and index HTTPX records in batches to Elasticsearch, handling large volumes efficiently.

        :param file_path: Path to the HTTPX log file
        :param watch: If True, watch the file for new lines and index them in real time
        :param chunk: Chunking configuration

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

        count = 0
        records = []

        with open(file_path, 'r') as file:
            for line in (file := follow(file) if watch else file):
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
                    count += 1
                    if len(records) >= chunk['batch']:
                            self.bulk_index(records, file_path, chunk, count)
                            records = []

        if records:
            self.bulk_index(records, file_path, chunk, count)

    def bulk_index(self, documents: list, file_path: str, chunk: dict, count: int):
        '''
        Index a batch of documents to Elasticsearch.
        
        :param documents: List of documents to index
        :param file_path: Path to the file being indexed
        :param count: Total number of records processed
        '''
        
        remaining_documents = documents

        parallel_bulk_config = {
            'client': self.es,
            'chunk_size': chunk['size'],
            'max_chunk_bytes': chunk['max_size'] * 1024 * 1024, # MB
            'thread_count': chunk['threads'],
            'queue_size': 2
        }

        while remaining_documents:
            failed_documents = []

            try:
                for success, response in helpers.parallel_bulk(actions=remaining_documents, **parallel_bulk_config):
                    if not success:
                        failed_documents.append(response)

                if not failed_documents:
                    ingested = parallel_bulk_config['chunk_size'] * parallel_bulk_config['thread_count']
                    logging.info(f'Successfully indexed {ingested:,} ({count:,} processed) records to {self.es_index} from {file_path}')
                    break

                else:
                    logging.warning(f'Failed to index {len(failed_documents):,} failed documents! Retrying...')
                    remaining_documents = failed_documents
            except Exception as e:
                logging.error(f'Failed to index documents! ({e})')
                time.sleep(30)


    def follow(file) -> str:
        '''
        Generator function that yields new lines in a file in real time.
        
        :param file: File object to read from
        '''

        file.seek(0,2) # Go to the end of the file

        while True:
            line = file.readline()

            if not line:
                time.sleep(0.1)
                continue

            yield line


def main():
    '''Main function when running this script directly.'''

    parser = argparse.ArgumentParser(description='Index data into Elasticsearch.')
    
    # General arguments
    parser.add_argument('input_path', help='Path to the input file or directory') # Required
    parser.add_argument('--dry-run', action='store_true', help='Dry run (do not index records to Elasticsearch)')
    parser.add_argument('--watch', action='store_true', help='Watch the input file for new lines and index them in real time')
    
    # Elasticsearch arguments
    parser.add_argument('--host', default='localhost', help='Elasticsearch host')
    parser.add_argument('--port', type=int, default=9200, help='Elasticsearch port')
    parser.add_argument('--user', default='elastic', help='Elasticsearch username')
    parser.add_argument('--password', default=os.getenv('ES_PASSWORD'), help='Elasticsearch password (if not provided, check environment variable ES_PASSWORD)')
    parser.add_argument('--api-key', help='Elasticsearch API Key for authentication')
    parser.add_argument('--self-signed', action='store_false', help='Elasticsearch is using self-signed certificates')

    # Elasticsearch indexing arguments
    parser.add_argument('--index', default='httpx-logs', help='Elasticsearch index name')
    parser.add_argument('--shards', type=int, default=1, help='Number of shards for the index')
    parser.add_argument('--replicas', type=int, default=1, help='Number of replicas for the index')

    # Performance arguments
    parser.add_argument('--batch-max', type=int, default=10, help='Maximum size in MB of a batch')
    parser.add_argument('--batch-size', type=int, default=5000, help='Number of records to index in a batch')
    parser.add_argument('--batch-threads', type=int, default=2, help='Number of threads to use when indexing in batches')
    parser.add_argument('--retries', type=int, default=10, help='Number of times to retry indexing a batch before failing')
    parser.add_argument('--timeout', type=int, default=30, help='Number of seconds to wait before retrying a batch')

    args = parser.parse_args()

    # Argument validation
    if not os.path.isdir(args.input_path) and not os.path.isfile(args.input_path):
        raise FileNotFoundError(f'Input path {args.input_path} does not exist or is not a file or directory')

    if not args.dry_run:
        if args.batch_size < 1:
            raise ValueError('Batch size must be greater than 0')
        
        elif args.retries < 1:
            raise ValueError('Number of retries must be greater than 0')
        
        elif args.timeout < 5:
            raise ValueError('Timeout must be greater than 4')
        
        elif args.batch_max < 1:
            raise ValueError('Batch max size must be greater than 0')
        
        elif args.batch_threads < 1:
            raise ValueError('Batch threads must be greater than 0')

        elif not args.host:
            raise ValueError('Missing required Elasticsearch argument: host')

        elif not args.api_key and (not args.user or not args.password):
            raise ValueError('Missing required Elasticsearch argument: either user and password or apikey')

        elif args.shards < 1:
            raise ValueError('Number of shards must be greater than 0')

        elif args.replicas < 0:
            raise ValueError('Number of replicas must be greater than 0')

    edx = ElasticIndexer(args.host, args.port, args.user, args.password, args.api_key, args.index, args.dry_run, args.self_signed, args.retries, args.timeout)
    
    if not args.dry_run:
        print(edx.get_cluster_health())
        
        time.sleep(3) # Delay to allow time for sniffing to complete

        nodes = edx.get_cluster_size()

        logging.info(f'Connected to {nodes:,} Elasticsearch node(s)')

        edx.create_index(args.shards, args.replicas) # Create the index if it does not exist

        chunk = {
            'size': args.batch_size,
            'max_size': args.batch_max * 1024 * 1024, # MB
            'threads': args.batch_threads
        }
        
        chunk['batch'] = nodes * (chunk['size'] * chunk['threads'])
    else:
        chunk = {} # Ugly hack to get this working...

    if os.path.isfile(args.input_path):
        logging.info(f'Processing file: {args.input_path}')
        edx.process_file(args.input_path, args.watch, chunk)

    elif os.path.isdir(args.input_path):
        count = 1
        total = len(os.listdir(args.input_path))
        logging.info(f'Processing {total:,} files in directory: {args.input_path}')
        for file in sorted(os.listdir(args.input_path)):
            file_path = os.path.join(args.input_path, file)
            if os.path.isfile(file_path):
                logging.info(f'[{count:,}/{total:,}] Processing file: {file_path}')
                edx.process_file(file_path, args.watch, chunk)
                count += 1
            else:
                logging.warning(f'[{count:,}/{total:,}] Skipping non-file: {file_path}')