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


    def create_index(self, shards: int = 1, replicas: int = 1):
        '''
        Create the Elasticsearch index with the defined mapping.
        
        :param shards: Number of shards for the index
        :param replicas: Number of replicas for the index
        '''

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
        Read and index DNS records in batches to Elasticsearch, handling large volumes efficiently.

        :param file_path: Path to the Masscan log file
        :param watch: If True, input file will be watched for new lines and indexed in real time\
        :param chunk: Chunk configuration for indexing in batches

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
                            if len(records) >= chunk['batch']:
                                self.bulk_index(records, file_path, chunk, count)
                                records = []

                    last_domain = domain

                    domain_records[domain] = {}

                if record_type not in domain_records[domain]:
                    domain_records[domain][record_type] = []

                domain_records[domain][record_type].append({'ttl': ttl, 'data': data})

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
    parser.add_argument('--index', default='dns-zones', help='Elasticsearch index name')
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



if __name__ == '__main__':
    main()