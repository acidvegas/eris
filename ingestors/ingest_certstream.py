#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingestors/ingest_certstream.py

import asyncio
import json
import logging
import time

try:
	import websockets
except ImportError:
	raise ImportError('Missing required \'websockets\' library. (pip install websockets)')


# Set a default elasticsearch index if one is not provided
default_index = 'eris-certstream'


def construct_map() -> dict:
	'''Construct the Elasticsearch index mapping for Certstream records.'''

	# Match on exact value or full text search
	keyword_mapping = { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

	# Construct the index mapping
	mapping = {
		'mappings': {
			'properties' : {
				'domain'      : keyword_mapping,
				'fingerprint' : keyword_mapping,
				'issuer'      : keyword_mapping,
				'subject'     : {
					'type'      : 'object',
					'properties': {
						'C':  { 'type': 'keyword' },
						'CN': { 'type': 'keyword' },
						'L':  { 'type': 'keyword' },
						'O':  { 'type': 'keyword' },
						'OU': { 'type': 'keyword' }
					}
				},
				'seen' : { 'type': 'date' }
			}
		}
	}

	return mapping


async def process_data(place_holder: str = None):
	'''
	Read and process Certsream records live from the Websocket stream.

	:param place_holder: Placeholder parameter to match the process_data function signature of other ingestors.
	'''

	# Loop until the user interrupts the process
	while True:
		try:

			# Connect to the Certstream websocket
			async for websocket in websockets.connect('wss://certstream.calidog.io', ping_interval=15, ping_timeout=60):

				# Read the websocket stream
				async for line in websocket:

					# Parse the JSON record
					try:
						record = json.loads(line)
					except json.decoder.JSONDecodeError:
						logging.error(f'Invalid line from the websocket: {line}')
						continue

					# Grab the unique domains from the records
					all_domains = set(record['data']['leaf_cert']['all_domains'])
					fingerprint = record['data']['leaf_cert']['fingerprint']
					issuer      = record['data']['leaf_cert']['issuer']['O']
					subject     = {k: v for k, v in record['data']['leaf_cert']['subject'].items() if v is not None}

					# Create a record for each domain
					for domain in all_domains:
						if domain.startswith('*.'):
							domain = domain[2:]
							if domain in all_domains:
								continue

						# Construct the document
						struct = {
							'domain'      : domain,
							'fingerprint' : fingerprint,
							'issuer'      : issuer,
							'subject'     : subject,
							'seen'        : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
						}

						yield {
							'_op_type'      : 'update',
							'_id'           : domain,
							'_index'        : default_index,
							'doc'           : struct,
							'doc_as_upsert' : True
						}

		except websockets.ConnectionClosed as e	:
			logging.error(f'Connection to Certstream was closed. Attempting to reconnect... ({e})')
			await asyncio.sleep(3)

		except Exception as e:
			logging.error(f'Error processing Certstream data: {e}')
			await asyncio.sleep(3)


async def test():
	'''Test the ingestion process.'''

	async for document in process_data():
		print(document)



if __name__ == '__main__':
	asyncio.run(test())