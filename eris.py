#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# eris.py

import asyncio
import argparse
import logging
import logging.handlers
import os
import stat
import sys
import json

sys.dont_write_bytecode = True # FUCKOFF __pycache__

try:
	from elasticsearch            import AsyncElasticsearch
	from elasticsearch.exceptions import NotFoundError
	from elasticsearch.helpers    import async_streaming_bulk
except ImportError:
	raise ImportError('Missing required \'elasticsearch\' library. (pip install elasticsearch)')


class ElasticIndexer:
	def __init__(self, args: argparse.Namespace):
		'''
		Initialize the Elastic Search indexer.

		:param args: Parsed arguments from argparse
		'''

		self.chunk_max  = args.chunk_max * 1024 * 1024 # MB
		self.chunk_size = args.chunk_size
		self.es_index   = args.index

		# Sniffing disabled due to an issue with the elasticsearch 8.x client (https://github.com/elastic/elasticsearch-py/issues/2005)
		es_config = {
			'hosts'               : [f'{args.host}:{args.port}'],
			#'hosts'                : [f'{args.host}:{port}' for port in ('9200',)], # Temporary alternative to sniffing
			'verify_certs'         : args.self_signed,
			'ssl_show_warn'        : args.self_signed,
			'request_timeout'      : args.timeout,
			'max_retries'          : args.retries,
			'retry_on_timeout'     : True,
			'http_compress'        : True,
			'connections_per_node' : 3 # Experiment with this value
			#'sniff_on_start': True,
			#'sniff_on_node_failure': True,
			#'min_delay_between_sniffing': 60
		}

		if args.api_key:
			es_config['api_key'] = (args.api_key, '') # Verify this is correct
		else:
			es_config['basic_auth'] = (args.user, args.password)

		self.es = AsyncElasticsearch(**es_config)


	async def close_connect(self):
		'''Close the Elasticsearch connection.'''

		await self.es.close()


	async def create_index(self, map_body: dict, pipeline: str = None, replicas: int = 1, shards: int = 1):
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
			'number_of_shards'   : shards,
			'number_of_replicas' : replicas
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


	async def process_data(self, file_path: str, data_generator: callable):
		'''
		Index records in chunks to Elasticsearch.

		:param file_path: Path to the file
		:param data_generator: Generator for the records to index
		'''

		count  = 0
		total  = 0
		errors = []

		try:
			async for ok, result in async_streaming_bulk(self.es, actions=data_generator(file_path), chunk_size=self.chunk_size, max_chunk_bytes=self.chunk_max,raise_on_error=False):
				action, result = result.popitem()

				if not ok:
					error_type   = result.get('error', {}).get('type',   'unknown')
					error_reason = result.get('error', {}).get('reason', 'unknown')
					logging.error('FAILED DOCUMENT:')
					logging.error(f'Error Type   : {error_type}')
					logging.error(f'Error Reason : {error_reason}')
					logging.error('Document     : ')
					logging.error(json.dumps(result, indent=2))
					input('Press Enter to continue...')
					errors.append(result)
					continue

				count += 1
				total += 1

				if count == self.chunk_size:
					logging.info(f'Successfully indexed {self.chunk_size:,} ({total:,} processed) records to {self.es_index} from {file_path}')
					count = 0

			if errors:
				raise Exception(f'{len(errors):,} document(s) failed to index. Check the logs above for details.')

		except Exception as e:
			raise Exception(f'Failed to index records to {self.es_index} from {file_path} ({e})')


def setup_logger(console_level: int = logging.INFO, file_level: int = None, log_file: str = 'debug.json', max_file_size: int = 10*1024*1024, backups: int = 5, ecs_format: bool = False):
	'''
	Setup the global logger for the application.

	:param console_level: Minimum level to capture logs to the console.
	:param file_level: Minimum level to capture logs to the file.
	:param log_file: File to write logs to.
	:param max_file_size: Maximum size of the log file before it is rotated.
	:param backups: Number of backup log files to keep.
	:param ecs_format: Use the Elastic Common Schema (ECS) format for logs.
	'''

	# Configure the root logger
	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG) # Minimum level to capture all logs

	# Clear existing handlers
	logger.handlers = []

	# Setup console handler
	console_handler = logging.StreamHandler()
	console_handler.setLevel(console_level)
	console_formatter = logging.Formatter('%(asctime)s | %(levelname)9s | %(message)s', '%I:%M:%S')
	console_handler.setFormatter(console_formatter)
	logger.addHandler(console_handler)

	# Setup rotating file handler if file logging is enabled
	if file_level is not None:
		file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=max_file_size, backupCount=backups)
		file_handler.setLevel(file_level)
  
		# Setup formatter to use ECS format if enabled or default format
		if ecs_format:
			try:
				from ecs_logging import StdlibFormatter
			except ImportError:
				raise ImportError('Missing required \'ecs-logging\' library. (pip install ecs-logging)')
			file_formatter = StdlibFormatter() # ECS formatter
		else:
			file_formatter = logging.Formatter('%(asctime)s | %(levelname)9s | %(message)s', '%Y-%m-%d %H:%M:%S')
        
		file_handler.setFormatter(file_formatter)
		logger.addHandler(file_handler)


async def main():
	'''Main function when running this script directly.'''

	parser = argparse.ArgumentParser(description='Elasticsearch Recon Ingestion Scripts (ERIS)')

	# General arguments
	parser.add_argument('input_path', help='Path to the input file or directory') # Required
	parser.add_argument('--watch', action='store_true', help='Create or watch a FIFO for real-time indexing')
	parser.add_argument('--log', choices=['debug', 'info', 'warning', 'error', 'critical'], help='Logging file level (default: disabled)')
	parser.add_argument('--ecs', action='store_true', default=False, help='Use the Elastic Common Schema (ECS) for logging')

	# Elasticsearch arguments
	parser.add_argument('--host', default='http://localhost', help='Elasticsearch host')
	parser.add_argument('--port', type=int, default=9200, help='Elasticsearch port')
	parser.add_argument('--user', default='elastic', help='Elasticsearch username')
	parser.add_argument('--password', default=os.getenv('ES_PASSWORD'), help='Elasticsearch password (if not provided, check environment variable ES_PASSWORD)')
	parser.add_argument('--api-key', default=os.getenv('ES_APIKEY'), help='Elasticsearch API Key for authentication (if not provided, check environment variable ES_APIKEY)')
	parser.add_argument('--self-signed', action='store_false', help='Elasticsearch is using self-signed certificates')

	# Elasticsearch indexing arguments
	parser.add_argument('--index', help='Elasticsearch index name')
	parser.add_argument('--pipeline', help='Use an ingest pipeline for the index')
	parser.add_argument('--replicas', type=int, default=1, help='Number of replicas for the index')
	parser.add_argument('--shards', type=int, default=1, help='Number of shards for the index')

	# Performance arguments
	parser.add_argument('--chunk-size', type=int, default=5000, help='Number of records to index in a chunk')
	parser.add_argument('--chunk-max', type=int, default=10485760, help='Maximum size of a chunk in bytes (default 10mb)')
	parser.add_argument('--retries', type=int, default=30, help='Number of times to retry indexing a chunk before failing')
	parser.add_argument('--timeout', type=int, default=60, help='Number of seconds to wait before retrying a chunk')

	# Ingestion arguments
	parser.add_argument('--certstream', action='store_true', help='Index Certstream records')
	parser.add_argument('--httpx', action='store_true', help='Index Httpx records')
	parser.add_argument('--masscan', action='store_true', help='Index Masscan records')
	parser.add_argument('--massdns', action='store_true', help='Index Massdns records')
	parser.add_argument('--zone', action='store_true', help='Index Zone records')
	parser.add_argument('--rir-delegations', action='store_true', help='Index RIR Delegations records')
	parser.add_argument('--rir-transfers', action='store_true', help='Index RIR Transfers records')

	args = parser.parse_args()

	if args.log:
		levels = {'debug': logging.DEBUG, 'info': logging.INFO, 'warning': logging.WARNING, 'error': logging.ERROR, 'critical': logging.CRITICAL}
		setup_logger(file_level=levels[args.log], log_file='eris.log', ecs_format=args.ecs)
	else:
		setup_logger()

	if args.host.endswith('/'):
		args.host = args.host[:-1]

	if args.watch:
		if not os.path.exists(args.input_path):
			os.mkfifo(args.input_path)
		elif not stat.S_ISFIFO(os.stat(args.input_path).st_mode):
			raise ValueError(f'Path {args.input_path} is not a FIFO')
	elif not os.path.isdir(args.input_path) and not os.path.isfile(args.input_path):
		raise FileNotFoundError(f'Input path {args.input_path} does not exist or is not a file or directory')

	logging.info(f'Connecting to Elasticsearch at {args.host}:{args.port}')

	edx = ElasticIndexer(args)

	if args.certstream:
		from ingestors import ingest_certstream      as ingestor
	elif args.httpx:
		from ingestors import ingest_httpx           as ingestor
	elif args.masscan:
		from ingestors import ingest_masscan         as ingestor
	elif args.massdns:
		from ingestors import ingest_massdns         as ingestor
	elif args.rir_delegations:
		from ingestors import ingest_rir_delegations as ingestor
	elif args.rir_transfers:
		from ingestors import ingest_rir_transfers   as ingestor
	elif args.zone:
		from ingestors import ingest_zone            as ingestor
	else:
		raise ValueError('No ingestor specified')

	health = await edx.es.cluster.health()
	logging.info(health)

	#await asyncio.sleep(5) # Delay to allow time for sniffing to complete (Sniffer temporarily disabled)

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

	await edx.close_connect() # Close the Elasticsearch connection to stop "Unclosed client session" warnings



if __name__ == '__main__':
	print('')
	print('┏┓┳┓┳┏┓   Elasticsearch Recon Ingestion Scripts')
	print('┣ ┣┫┃┗┓        Developed by Acidvegas in Python')
	print('┗┛┛┗┻┗┛             https://git.acid.vegas/eris')
	print('')
	asyncio.run(main())
