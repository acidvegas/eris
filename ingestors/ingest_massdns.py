#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)

# Massdns Log File Ingestion:
#
# This script takes JSON formatted massdns logs and indexes them into Elasticsearch.
#
# Saving my "typical" massdns command here for reference to myself:
#   python $HOME/massdns/scripts/ptr.py | massdns -r $HOME/massdns/nameservers.txt -t PTR --filter NOERROR -o J -w $HOME/massdns/ptr.json

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
        Read and index PTR records in batches to Elasticsearch, handling large volumes efficiently.

        :param file_path: Path to the PTR log file
        :param batch_size: Number of records to process before indexing

        Example PTR record:
        {
            "name":"0.0.50.73.in-addr.arpa.",
            "type":"PTR",
            "class":"IN",
            "status":"NOERROR",
            "rx_ts":1704595370817617348,
            "data": {
                "answers": [
                        {"ttl":3600,"type":"PTR","class":"IN","name":"0.0.50.73.in-addr.arpa.","data":"c-73-50-0-0.hsd1.il.comcast.net."}
                    ],
                "authorities": [
                    {"ttl":86400,"type":"NS","class":"IN","name":"73.in-addr.arpa.","data":"dns102.comcast.net."},
                    {"ttl":86400,"type":"NS","class":"IN","name":"73.in-addr.arpa.","data":"dns103.comcast.net."},
                    {"ttl":86400,"type":"NS","class":"IN","name":"73.in-addr.arpa.","data":"dns105.comcast.net."},
                    {"ttl":86400,"type":"NS","class":"IN","name":"73.in-addr.arpa.","data":"dns101.comcast.net."},
                    {"ttl":86400,"type":"NS","class":"IN","name":"73.in-addr.arpa.","data":"dns104.comcast.net."}
                ],
                "additionals":[
                    {"ttl":105542,"type":"A","class":"IN","name":"dns101.comcast.net.","data":"69.252.250.103"},
                    {"ttl":105542,"type":"A","class":"IN","name":"dns102.comcast.net.","data":"68.87.85.132"},
                    {"ttl":105542,"type":"AAAA","class":"IN","name":"dns104.comcast.net.","data":"2001:558:100a:5:68:87:68:244"},
                    {"ttl":105542,"type":"AAAA","class":"IN","name":"dns105.comcast.net.","data":"2001:558:100e:5:68:87:72:244"}
                ]
            },
            "flags": ["rd","ra"],
            "resolver":"103.144.64.173:53",
            "proto":"UDP"
        }
        '''

        records = []

        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip()

                if not line:
                    continue

                record = json.loads(line.strip())

                # Should we keep CNAME records? Can an IP address have a CNAME?
                if record['type'] != 'PTR':
                    logging.error(record)
                    raise ValueError(f'Unsupported record type: {record["type"]}')
               
                if record['class'] != 'IN':
                    logging.error(record)
                    raise ValueError(f'Unsupported record class: {record["class"]}')

                if record['status'] != 'NOERROR':
                    logging.warning(f'Skipping bad record: {record}')
                    continue

                record['ip'] = '.'.join(record['name'].replace('.in-addr.arpa', '').split('.')[::-1])
                record['name'] = record['name'].rstrip('.')
                record['timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.pop('rx_ts') / 1_000_000_000))
                
                if self.dry_run:
                    print(record)
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
        
        if args.shards < 1:
            raise ValueError('Number of shards must be greater than 0')
        
        if args.replicas < 1:
            raise ValueError('Number of replicas must be greater than 0')
        
        logging.info(f'Connecting to Elasticsearch at {args.host}:{args.port}...')

    edx = ElasticIndexer(args.host, args.port, args.user, args.password, args.api_key, args.index, args.dry_run, args.self_signed)

    if not args.dry_run:
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