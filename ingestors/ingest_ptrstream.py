#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingestors/ingest_ptrstream.py

'''
This ingestor is meant to be used with https://github.com/acidvegas/ptrstream
'''

import json
import logging

try:
	import aiofiles
except ImportError:
	raise ImportError('Missing required \'aiofiles\' library. (pip install aiofiles)')


# Set a default elasticsearch index if one is not provided
default_index = 'havoc-ptr'


def construct_map() -> dict:
	'''Construct the Elasticsearch index mapping for records'''

	# Match on exact value or full text search
	keyword_mapping = { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

	# Construct the index mapping
	mapping = {
		'mappings': {
			'properties': {
				'ip'          : {'type': 'ip'},
				'nameserver'  : {'type': 'ip'},
				'record'      : keyword_mapping,
				'record_type' : {'type': 'keyword'},
				'ttl'         : {'type': 'integer'},
				'seen'        : {'type': 'date'}
			}
		}
	}

	return mapping


async def process_data(input_path: str):
	'''
	Read and process the input file

	:param input_path: Path to the input file
	'''

	# Open the input file
	async with aiofiles.open(input_path) as input_file:

		# Read the input file line by line
		async for line in input_file:

			# Strip whitespace
			line = line.strip()

			# Skip empty lines and lines that do not start with a JSON object
			if not line or not line.startswith('{'):
				continue

			# Parse the JSON record
			try:
				record = json.loads(line)
			except json.decoder.JSONDecodeError:
				logging.error(f'Failed to parse JSON record! ({line})')
				continue

			# Get the IP address from the in-addr.arpa domain
			ip = record['ip']

			# Do not index PTR records that have the same record as the in-addr.arpa domain
			if record['record'] == '.'.join(ip.split('.')[::-1]) + '.in-addr.arpa':
				continue

			# Create the document structure
			yield {
				'_op_type'      : 'update',
				'_id'           : ip,
				'_index'        : default_index,
				'doc'           : record,
				'doc_as_upsert' : True # Create the document if it does not exist
			}


async def test(input_path: str):
	'''
	Test the ingestion process

	:param input_path: Path to the input file
	'''

	async for document in process_data(input_path):
		print(document)



if __name__ == '__main__':
	import argparse
	import asyncio

	parser = argparse.ArgumentParser(description='Ingestor for ERIS')
	parser.add_argument('input_path', help='Path to the input file or directory')
	args = parser.parse_args()

	asyncio.run(test(args.input_path))