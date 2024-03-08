#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# eris.py

import asyncio
import argparse
import logging
import os
import stat
import sys

sys.dont_write_bytecode = True

try:
    # This is commented out because there is a bug with the elasticsearch library that requires a patch (see initialize() method below)
    #from elasticsearch import AsyncElasticsearch
    from elasticsearch.exceptions import NotFoundError
    from elasticsearch.helpers import async_streaming_bulk
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

        self.chunk_max  = args.chunk_max * 1024 * 1024 # MB
        self.chunk_size = args.chunk_size
        self.es = None
        self.es_index = args.index

        self.es_config = {
            'hosts': [f'{args.host}:{args.port}'],
            'verify_certs': args.self_signed,
            'ssl_show_warn': args.self_signed,
            'request_timeout': args.timeout,
            'max_retries': args.retries,
            'retry_on_timeout': True,
            'sniff_on_start': False, # Problems when True....
            'sniff_on_node_failure': True,
            'min_delay_between_sniffing': 60 # Add config option for this?
        }

        #if args.api_key:
        #    self.es_config['api_key'] = (args.api_key, '') # Verify this is correct
        #else:
        self.es_config['basic_auth'] = (args.user, args.password)


    async def initialize(self):
        '''Initialize the Elasticsearch client.'''

        # Patching the Elasticsearch client to fix a bug with sniffing (https://github.com/elastic/elasticsearch-py/issues/2005#issuecomment-1645641960)
        import sniff_patch
        self.es = await sniff_patch.init_elasticsearch(**self.es_config)

        # Remove the above and uncomment the below if the bug is fixed in the Elasticsearch client:
        #self.es = AsyncElasticsearch(**es_config)


    async def create_index(self, map_body: dict, pipeline: str = '', replicas: int = 1, shards: int = 1):
        '''
        Create the Elasticsearch index with the defined mapping.

        :param map_body: Mapping for the index
        :param pipeline: Name of the ingest pipeline to use for the index
        :param replicas: Number of replicas for the index
        :param shards: Number of shards for the index
        '''

        if await self.es.indices.exists(index=self.es_index):
            logging.info(f'Index \'{self.es_index}\' already exists.')
            return

        mapping = map_body

        mapping['settings'] = {
            'number_of_shards': shards,
            'number_of_replicas': replicas
        }

        if pipeline:
            try:
                await self.es.ingest.get_pipeline(id=pipeline)
                logging.info(f'Using ingest pipeline \'{pipeline}\' for index \'{self.es_index}\'')
                mapping['settings']['index.default_pipeline'] = pipeline
            except NotFoundError:
                raise ValueError(f'Ingest pipeline \'{pipeline}\' does not exist.')

        response = await self.es.indices.create(index=self.es_index, body=mapping)

        if response.get('acknowledged') and response.get('shards_acknowledged'):
            logging.info(f'Index \'{self.es_index}\' successfully created.')
        else:
            raise Exception(f'Failed to create index. ({response})')


    async def get_cluster_health(self) -> dict:
        '''Get the health of the Elasticsearch cluster.'''

        return await self.es.cluster.health()


    async def get_cluster_size(self) -> int:
        '''Get the number of nodes in the Elasticsearch cluster.'''

        cluster_stats = await self.es.cluster.stats()
        number_of_nodes = cluster_stats['nodes']['count']['total']

        return number_of_nodes


    async def process_data(self, file_path: str, data_generator: callable):
        '''
        Index records in chunks to Elasticsearch.

        :param file_path: Path to the file
        :param index_name: Name of the index
        :param data_generator: Generator for the records to index
        '''

        count = 0
        total = 0

        async for ok, result in async_streaming_bulk(self.es, actions=data_generator(file_path), chunk_size=self.chunk_size, max_chunk_bytes=self.chunk_max):
            action, result = result.popitem()

            if not ok:
                logging.error(f'Failed to index document ({result["_id"]}) to {self.es_index} from {file_path} ({result})')
                input('Press Enter to continue...') # Debugging (will possibly remove this since we have retries enabled)
                continue

            count += 1
            total += 1

            if count == self.chunk_size:
                logging.info(f'Successfully indexed {self.chunk_size:,} ({total:,} processed) records to {self.es_index} from {file_path}')
                count = 0

        logging.info(f'Finished indexing {total:,} records to {self.es_index} from {file_path}')


async def main():
    '''Main function when running this script directly.'''

    parser = argparse.ArgumentParser(description='Index data into Elasticsearch.')

    # General arguments
    parser.add_argument('input_path', help='Path to the input file or directory') # Required
    parser.add_argument('--watch', action='store_true', help='Create or watch a FIFO for real-time indexing')

    # Elasticsearch arguments
    parser.add_argument('--host', default='http://localhost/', help='Elasticsearch host')
    parser.add_argument('--port', type=int, default=9200, help='Elasticsearch port')
    parser.add_argument('--user', default='elastic', help='Elasticsearch username')
    parser.add_argument('--password', default=os.getenv('ES_PASSWORD'), help='Elasticsearch password (if not provided, check environment variable ES_PASSWORD)')
    #parser.add_argument('--api-key', default=os.getenv('ES_APIKEY'), help='Elasticsearch API Key for authentication (if not provided, check environment variable ES_APIKEY)')
    parser.add_argument('--self-signed', action='store_false', help='Elasticsearch is using self-signed certificates')

    # Elasticsearch indexing arguments
    parser.add_argument('--index', help='Elasticsearch index name')
    parser.add_argument('--pipeline', help='Use an ingest pipeline for the index')
    parser.add_argument('--replicas', type=int, default=1, help='Number of replicas for the index')
    parser.add_argument('--shards', type=int, default=1, help='Number of shards for the index')

    # Performance arguments
    parser.add_argument('--chunk-size', type=int, default=50000, help='Number of records to index in a chunk')
    parser.add_argument('--chunk-max', type=int, default=100, help='Maximum size of a chunk in bytes')
    parser.add_argument('--retries', type=int, default=100, help='Number of times to retry indexing a chunk before failing')
    parser.add_argument('--timeout', type=int, default=60, help='Number of seconds to wait before retrying a chunk')

    # Ingestion arguments
    parser.add_argument('--cert', action='store_true', help='Index Certstream records')
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
    await edx.initialize() # Initialize the Elasticsearch client asyncronously

    if args.cert:
        from ingestors import ingest_certs   as ingestor
    if args.httpx:
        from ingestors import ingest_httpx   as ingestor
    elif args.masscan:
        from ingestors import ingest_masscan as ingestor
    elif args.massdns:
        from ingestors import ingest_massdns as ingestor
    elif args.zone:
        from ingestors import ingest_zone    as ingestor

    if not isinstance(ingestor, object):
        raise ValueError('No ingestor selected')

    health = await edx.get_cluster_health()
    print(health)

    await asyncio.sleep(5) # Delay to allow time for sniffing to complete

    nodes = await edx.get_cluster_size()
    logging.info(f'Connected to {nodes:,} Elasticsearch node(s)')

    if not edx.es_index:
        edx.es_index = ingestor.default_index

    map_body = ingestor.construct_map()
    await edx.create_index(map_body, args.pipeline, args.replicas, args.shards)

    if os.path.isfile(args.input_path):
        logging.info(f'Processing file: {args.input_path}')
        await edx.process_data(args.input_path, ingestor.process_data)

    elif stat.S_ISFIFO(os.stat(args.input_path).st_mode):
        logging.info(f'Watching FIFO: {args.input_path}')
        await edx.process_data(args.input_path, ingestor.process_data)

    elif os.path.isdir(args.input_path):
        count = 1
        total = len(os.listdir(args.input_path))
        logging.info(f'Processing {total:,} files in directory: {args.input_path}')
        for file in sorted(os.listdir(args.input_path)):
            file_path = os.path.join(args.input_path, file)
            if os.path.isfile(file_path):
                logging.info(f'[{count:,}/{total:,}] Processing file: {file_path}')
                await edx.process_data(file_path, ingestor.process_data)
                count += 1
            else:
                logging.warning(f'[{count:,}/{total:,}] Skipping non-file: {file_path}')



if __name__ == '__main__':
    asyncio.run(main())
