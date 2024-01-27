#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)

# DNS Zone File Ingestion:
#
# This script will read a DNS zone file and index the records to Elasticsearch.
#
# Zone files must be valid and follow the RFC 1035 standard for proper indexing:
#   - https://datatracker.ietf.org/doc/html/rfc1035
#
# Most of my zones come from CZDS but AXFR outputs can be used as well.
# Anomaly detection is in place to alert the user of any unexpected records.


# WARNING NOTICE:
# 
# The following zones need reindex due to previous unsupported record types:
#   - .wang (Contains a completely invalid line) (tjjm6hs65gL9KUFU76J747MB NS)
#   - .tel  (NAPTR records were missing)
#   - .nu   (RP records were missing
#   - .se   (RP records were missing)

import argparse
import logging
import os
import time

try:
    from elasticsearch import Elasticsearch, helpers, ConnectionError, TransportError
except ImportError:
    raise ImportError('Missing required \'elasticsearch\' library. (pip install elasticsearch)')


# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d %I:%M:%S')

# Record types to index
record_types = ('a','aaaa','caa','cdnskey','cds','cname','dnskey','ds','mx','naptr','ns','nsec','nsec3','nsec3param','ptr','rrsig','rp','sshfp','soa','srv','txt','type65534')


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
                    'domain':  { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } },
                    'records': { 'properties': {} },
                    'seen':    {'type': 'date'}
                }
            }
        }

        # Add record types to mapping dynamically to not clutter the code
        for item in record_types:
            if item in ('a','aaaa'):
                mapping['mappings']['properties']['records']['properties'][item] = {
                    'properties': {
                        'data': { 'type': 'ip' },
                        'ttl':  { 'type': 'integer' }
                    }
                }
            else:
                mapping['mappings']['properties']['records']['properties'][item] = {
                    'properties': {
                    'data': { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } },
                    'ttl':  { 'type': 'integer' }
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
        Read and index DNS records in batches to Elasticsearch, handling large volumes efficiently.

        :param file_path: Path to the DNS zone file
        :param batch_size: Number of records to process before indexing

        Example record:
        0so9l9nrl425q3tf7dkv1nmv2r3is6vm.vegas. 3600    in  nsec3   1 1 100 332539EE7F95C32A 10MHUKG4FHIAVEFDOTF6NKU5KFCB2J3A NS DS RRSIG
        0so9l9nrl425q3tf7dkv1nmv2r3is6vm.vegas. 3600    in  rrsig   NSEC3 8 2 3600 20240122151947 20240101141947 4125 vegas. hzIvQrZIxBSwRWyiHkb5M2W0R3ikNehv884nilkvTt9DaJSDzDUrCtqwQb3jh6+BesByBqfMQK+L2n9c//ZSmD5/iPqxmTPCuYIB9uBV2qSNSNXxCY7uUt5w7hKUS68SLwOSjaQ8GRME9WQJhY6gck0f8TT24enjXXRnQC8QitY=
        1-800-flowers.vegas.    3600    in  ns  dns1.cscdns.net.
        1-800-flowers.vegas.    3600    in  ns  dns2.cscdns.net.
        100.vegas.  3600    in  ns  ns51.domaincontrol.com.
        100.vegas.  3600    in  ns  ns52.domaincontrol.com.
        1001.vegas. 3600    in  ns  ns11.waterrockdigital.com.
        1001.vegas. 3600    in  ns  ns12.waterrockdigital.com.

        Will be indexed as:
        {
            "domain": "1001.vegas",
            "records": {
                "ns": [
                    {"ttl": 3600, "data": "ns11.waterrockdigital.com"},
                    {"ttl": 3600, "data": "ns12.waterrockdigital.com"}
                ]
            },
            "seen": "2021-09-01T00:00:00Z" # Zulu time added upon indexing
        }
        '''

        count = 0
        records = []
        domain_records = {}
        last_domain = None

        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip()

                if not line or line.startswith(';'):
                    continue

                parts = line.split()

                if len(parts) < 5:
                    raise ValueError(f'Invalid line: {line}')

                domain, ttl, record_class, record_type, data = parts[0].rstrip('.').lower(), parts[1], parts[2].lower(), parts[3].lower(), ' '.join(parts[4:])

                if not ttl.isdigit():
                    raise ValueError(f'Invalid TTL: {ttl} with line: {line}')
                
                ttl = int(ttl)

                if record_class != 'in':
                    raise ValueError(f'Unsupported record class: {record_class} with line: {line}') # Anomaly (Doubtful any CHAOS/HESIOD records will be found)

                # We do not want to collide with our current mapping (Again, this is an anomaly)
                if record_type not in record_types:
                    raise ValueError(f'Unsupported record type: {record_type} with line: {line}')

                # Little tidying up for specific record types
                if record_type == 'nsec':
                    data = ' '.join([data.split()[0].rstrip('.'), *data.split()[1:]])
                elif record_type == 'soa':
                     data = ' '.join([part.rstrip('.') if '.' in part else part for part in data.split()])
                elif data.endswith('.'):
                    data = data.rstrip('.')

                if domain != last_domain:
                    if last_domain:
                        source = {'domain': last_domain, 'records': domain_records[last_domain], 'seen': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
                        
                        del domain_records[last_domain]

                        if self.dry_run:
                            print(source)
                        else:
                            struct = {'_index': self.es_index, '_source': source}
                            records.append(struct)
                            count += 1
                            if len(records) >= batch_size:
                                while True:
                                    try:
                                        success, _ = helpers.bulk(self.es, records)
                                    except (ConnectionError, TransportError) as e:
                                        logging.error(f'Failed to index records to Elasticsearch. ({e})')
                                        time.sleep(60)
                                    else:
                                        logging.info(f'Successfully indexed {success:,} ({count:,}) records to {self.es_index} from {file_path}')
                                        records = []
                                        break

                    last_domain = domain

                    domain_records[domain] = {}

                if record_type not in domain_records[domain]:
                    domain_records[domain][record_type] = []

                domain_records[domain][record_type].append({'ttl': ttl, 'data': data})

        if records:
            while True:
                try:
                    success, _ = helpers.bulk(self.es, records)
                except (ConnectionError, TransportError) as e:
                    logging.error(f'Failed to index records to Elasticsearch. ({e})')
                    time.sleep(60)
                else:
                    logging.info(f'Successfully indexed {success:,} ({count:,}) records to {self.es_index} from {file_path}')
                    break


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
    parser.add_argument('--index', default='dns-zones', help='Elasticsearch index name')
    parser.add_argument('--shards', type=int, default=1, help='Number of shards for the index') # This depends on your cluster configuration
    parser.add_argument('--replicas', type=int, default=1, help='Number of replicas for the index') # This depends on your cluster configuration

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