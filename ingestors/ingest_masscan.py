#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)

# Masscan Log File Ingestion:
#
# This script takes JSON formatted masscan logs with banners and indexes them into Elasticsearch.
#
# Saving my "typical" masscan command here for reference to myself:
#   masscan 0.0.0.0/0 -p80,443 --banners --open-only --rate 50000 --shard 1/10 --excludefile exclude.conf -oJ output.json --interactive

import argparse
import json
import logging
import os
import re
import time

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


    def create_index(self, shards: int = 1, replicas: int = 1):
        '''Create the Elasticsearch index with the defined mapping.'''

        mapping = {
            'settings': {
                'number_of_shards': shards,
                'number_of_replicas': replicas
            },
            'mappings': {
                'properties': {
                    'ip':      { 'type': 'ip' },
                    'port':    { 'type': 'integer' },
                    'proto':   { 'type': 'keyword' },
                    'service': { 'type': 'keyword' },
                    'banner':  { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } },
                    'ref_id':  { 'type': 'keyword' },
                    'seen':    { 'type': 'date' }
                }
            }
        }

        if not self.es.indices.exists(index=self.es_index):
            response = self.es.indices.create(index=self.es_index, body=mapping)
            
            if response.get('acknowledged') and response.get('shards_acknowledged'):
                logging.info(f'Index \'{self.es_index}\' successfully created.')
            else:
                raise Exception(f'Failed to create index. ({response})')

        else:
            logging.warning(f'Index \'{self.es_index}\' already exists.')
   

    def process_file(self, file_path: str, batch_size: int):
        '''
        Read and index Masscan records in batches to Elasticsearch, handling large volumes efficiently.

        :param file_path: Path to the Masscan log file
        :param batch_size: Number of records to process before indexing

        Example record:
        {
            "ip": "43.134.51.142",
            "timestamp": "1705255468", # Convert to ZULU BABY
            "ports": [ # Typically only one port per record, but we will create a record for each port opened
                {
                    "port": 22,
                    "proto": "tcp",
                    "service": { # This field is optional
                        "name": "ssh",
                        "banner": "SSH-2.0-OpenSSH_8.9p1 Ubuntu-3ubuntu0.4"
                    }
                }
            ]
        }

        Will be indexed as:
        {
            "ip": "43.134.51.142",
            "port": 22,
            "proto": "tcp",
            "service": "ssh", # Optional: not every record will have a service name ("unknown" is ignored)
            "banner": "SSH-2.0-OpenSSH_8.9p1 Ubuntu-3ubuntu0.4", # Optional: not every record will have a banner
            "seen": "2021-10-08T02:04:28Z",
            "ref_id": "?sKfOvsC4M4a2W8PaC4zF?" # This is optional and will only be present if the banner contains a reference ID (TCP RST Payload, Might be useful?)
        }
        '''

        records = []
        
        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip()

                if not line or not line.startswith('{'):
                    continue

                record = json.loads(line)

                for port_info in record['ports']:
                    struct = {
                        'ip': record['ip'],
                        'port': port_info['port'],
                        'proto': port_info['proto'],
                        'seen': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(int(record['timestamp']))),
                    }

                    if 'service' in port_info:
                        if 'name' in port_info['service']:
                            if port_info['service']['name'] != 'unknown':
                                struct['service'] = port_info['service']['name']

                        if 'banner' in port_info['service']:
                            banner = port_info['service']['banner']
                            match = re.search(r'\(Ref\.Id: (.*?)\)', banner)
                            if match:
                                struct['ref_id'] = match.group(1)
                            else:
                                struct['banner'] = banner
                    
                    if self.dry_run:
                        print(struct)
                    else:
                        struct = {'_index': self.es_index, '_source': struct}
                        records.append(struct)

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

    # Elasticsearch arguments
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

    edx.create_index(args.shards, args.replicas) # Create the index if it does not exist

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