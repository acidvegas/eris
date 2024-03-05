#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)

import argparse
import logging
import os
import stat
import time
import sys

sys.dont_write_bytecode = True

try:
    from elasticsearch import Elasticsearch, helpers
    from elasticsearch.exceptions import NotFoundError
except ImportError:
    raise ImportError('Missing required \'elasticsearch\' library. (pip install elasticsearch)')

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d %I:%M:%S')


class ElasticIndexer:
    def __init__(self, args: argparse.Namespace):
        '''
        Initialize the Elastic Search indexer.

        :param args: Parsed arguments from argparse
        '''

        self.chunk_max = args.chunk_max * 1024 * 1024 # MB
        self.chunk_size = args.chunk_size
        self.chunk_threads = args.chunk_threads
        self.dry_run = args.dry_run
        self.es_index = args.index

        if not args.dry_run:
            es_config = {
                'hosts': [f'{args.host}:{args.port}'],
                'verify_certs': args.self_signed,
                'ssl_show_warn': args.self_signed,
                'request_timeout': args.timeout,
                'max_retries': args.retries,
                'retry_on_timeout': True,
                'sniff_on_start': False,
                'sniff_on_node_failure': True,
                'min_delay_between_sniffing': 60 # Add config option for this?
            }

            if args.api_key:
                es_config['headers'] = {'Authorization': f'ApiKey {args.api_key}'}
            else:
                es_config['basic_auth'] = (args.user, args.password)
                
            # Patching the Elasticsearch client to fix a bug with sniffing (https://github.com/elastic/elasticsearch-py/issues/2005#issuecomment-1645641960)
            import sniff_patch
            self.es = sniff_patch.init_elasticsearch(**es_config)

            # Remove the above and uncomment the below if the bug is fixed in the Elasticsearch client:
            #self.es = Elasticsearch(**es_config)


    def create_index(self, map_body: dict, pipeline: str = '', replicas: int = 1, shards: int = 1, ):
        '''
        Create the Elasticsearch index with the defined mapping.
        
        :param pipline: Name of the ingest pipeline to use for the index
        :param replicas: Number of replicas for the index
        :param shards: Number of shards for the index
        '''

        if self.es.indices.exists(index=self.es_index):
            logging.info(f'Index \'{self.es_index}\' already exists.')
            return

        mapping = map_body

        mapping['settings'] = {
            'number_of_shards': shards,
            'number_of_replicas': replicas
        }

        if pipeline:
            try:
                self.es.ingest.get_pipeline(id=pipeline)
                logging.info(f'Using ingest pipeline \'{pipeline}\' for index \'{self.es_index}\'')
                mapping['settings']['index.default_pipeline'] = pipeline
            except NotFoundError:
                raise ValueError(f'Ingest pipeline \'{pipeline}\' does not exist.')

        response = self.es.indices.create(index=self.es_index, body=mapping)

        if response.get('acknowledged') and response.get('shards_acknowledged'):
            logging.info(f'Index \'{self.es_index}\' successfully created.')
        else:
            raise Exception(f'Failed to create index. ({response})')


    def get_cluster_health(self) -> dict:
        '''Get the health of the Elasticsearch cluster.'''

        return self.es.cluster.health()
    

    def get_cluster_size(self) -> int:
        '''Get the number of nodes in the Elasticsearch cluster.'''

        cluster_stats = self.es.cluster.stats()
        number_of_nodes = cluster_stats['nodes']['count']['total']

        return number_of_nodes


    def bulk_index(self, documents: list, file_path: str, count: int):
        '''
        Index a batch of documents to Elasticsearch.
        
        :param documents: List of documents to index
        :param file_path: Path to the file being indexed
        :param count: Total number of records processed
        '''

        remaining_documents = documents

        parallel_bulk_config = {
            'client': self.es,
            'chunk_size': self.chunk_size,
            'max_chunk_bytes': self.chunk_max,
            'thread_count': self.chunk_threads,
            'queue_size': 2 # Add config option for this?
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
                time.sleep(30) # Should we add a config option for this?


    def process_file(self, file_path: str, batch_size: int, ingest_function: callable):
        '''
        Read and index records in batches to Elasticsearch.

        :param file_path: Path to the file
        :param batch_size: Number of records to index per batch
        :param ingest_function: Function to process the file
        '''

        count = 0
        records = []

        for processed in ingest_function(file_path):

            if not processed:
                break

            if self.dry_run:
                print(processed)
                continue

            struct = {'_index': self.es_index, '_source': processed}
            records.append(struct)
            count += 1
            
            if len(records) >= batch_size:
                self.bulk_index(records, file_path, count)
                records = []

        if records:
            self.bulk_index(records, file_path, count)


def main():
    '''Main function when running this script directly.'''

    parser = argparse.ArgumentParser(description='Index data into Elasticsearch.')
    
    # General arguments
    parser.add_argument('input_path', help='Path to the input file or directory') # Required
    parser.add_argument('--dry-run', action='store_true', help='Dry run (do not index records to Elasticsearch)')
    parser.add_argument('--watch', action='store_true', help='Create or watch a FIFO for real-time indexing')
    
    # Elasticsearch arguments
    parser.add_argument('--host', default='localhost', help='Elasticsearch host')
    parser.add_argument('--port', type=int, default=9200, help='Elasticsearch port')
    parser.add_argument('--user', default='elastic', help='Elasticsearch username')
    parser.add_argument('--password', default=os.getenv('ES_PASSWORD'), help='Elasticsearch password (if not provided, check environment variable ES_PASSWORD)')
    parser.add_argument('--api-key', default=os.getenv('ES_APIKEY'), help='Elasticsearch API Key for authentication (if not provided, check environment variable ES_APIKEY)')
    parser.add_argument('--self-signed', action='store_false', help='Elasticsearch is using self-signed certificates')

    # Elasticsearch indexing arguments
    parser.add_argument('--index', help='Elasticsearch index name')
    parser.add_argument('--pipeline', help='Use an ingest pipeline for the index')
    parser.add_argument('--replicas', type=int, default=1, help='Number of replicas for the index')
    parser.add_argument('--shards', type=int, default=3, help='Number of shards for the index')
    
    # Performance arguments
    parser.add_argument('--chunk-max', type=int, default=10, help='Maximum size in MB of a chunk')
    parser.add_argument('--chunk-size', type=int, default=50000, help='Number of records to index in a chunk')
    parser.add_argument('--chunk-threads', type=int, default=3, help='Number of threads to use when indexing in chunks')
    parser.add_argument('--retries', type=int, default=60, help='Number of times to retry indexing a chunk before failing')
    parser.add_argument('--timeout', type=int, default=30, help='Number of seconds to wait before retrying a chunk')

    # Ingestion arguments
    parser.add_argument('--httpx', action='store_true', help='Index Httpx records')
    parser.add_argument('--masscan', action='store_true', help='Index Masscan records')
    parser.add_argument('--massdns', action='store_true', help='Index Massdns records')
    parser.add_argument('--zone', action='store_true', help='Index Zone records')

    args = parser.parse_args()

    if args.watch:
        if not os.path.exists(args.input_path):
            os.mkfifo(args.input_path)
        elif os.path.exists(args.input_path) and stat.S_ISFIFO(os.stat(args.input_path).st_mode):
            raise ValueError(f'Path {args.input_path} is not a FIFO')
    elif not os.path.isdir(args.input_path) and not os.path.isfile(args.input_path):
        raise FileNotFoundError(f'Input path {args.input_path} does not exist or is not a file or directory')

    edx = ElasticIndexer(args)

    if args.httpx:
        from ingestors import ingest_httpx   as ingestor
    elif args.masscan:
        from ingestors import ingest_masscan as ingestor
    elif args.massdns:
        from ingestors import ingest_massdns as ingestor
    elif args.zone:
        from ingestors import ingest_zone    as ingestor

    batch_size = 0
    
    if not args.dry_run:
        print(edx.get_cluster_health())

        time.sleep(3) # Delay to allow time for sniffing to complete
        
        nodes = edx.get_cluster_size()
        logging.info(f'Connected to {nodes:,} Elasticsearch node(s)')

        if not edx.es_index:
            edx.es_index = ingestor.default_index

        map_body = ingestor.construct_map()
        edx.create_index(map_body, args.pipeline, args.replicas, args.shards)

        batch_size = int(nodes * (args.chunk_size * args.chunk_threads))
    
    if os.path.isfile(args.input_path):
        logging.info(f'Processing file: {args.input_path}')
        edx.process_file(args.input_path, batch_size, ingestor.process_file)

    elif stat.S_ISFIFO(os.stat(args.input_path).st_mode):
        logging.info(f'Watching FIFO: {args.input_path}')
        edx.process_file(args.input_path, batch_size, ingestor.process_file)

    elif os.path.isdir(args.input_path):
        count = 1
        total = len(os.listdir(args.input_path))
        logging.info(f'Processing {total:,} files in directory: {args.input_path}')
        for file in sorted(os.listdir(args.input_path)):
            file_path = os.path.join(args.input_path, file)
            if os.path.isfile(file_path):
                logging.info(f'[{count:,}/{total:,}] Processing file: {file_path}')
                edx.process_file(file_path, batch_size, ingestor.process_file)
                count += 1
            else:
                logging.warning(f'[{count:,}/{total:,}] Skipping non-file: {file_path}')



if __name__ == '__main__':
    main()