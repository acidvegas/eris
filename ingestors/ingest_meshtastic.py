#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_meshtastic.py

import asyncio
import json
import logging

try:
	import aiofiles
except ImportError:
	raise ImportError('Missing required \'aiofiles\' library. (pip install aiofiles)')


# Set a default elasticsearch index if one is not provided
default_index = 'eris-meshtastic'


def construct_map() -> dict:
	'''Construct the Elasticsearch index mapping for Meshtastic records.'''

	# Mapping not done yet
	return {}


async def process_data(input_path: str):
	'''
	Read and process the input file

	:param input_path: Path to the input file
	'''

	async with aiofiles.open(input_path) as input_file:
		# Read the input file line by line
		async for line in input_file:
			line = line.strip()

			# Sentinel value to indicate the end of a process (for closing out a FIFO stream)
			if line == '~eof':
				break

			# Skip empty lines and lines that do not start with a JSON object
			if not line or not line.startswith('{'):
				continue

			# Parse the JSON record
			try:
				record = json.loads(line)
			except json.decoder.JSONDecodeError:
				logging.error(f'Failed to parse JSON record! ({line})')
				continue

			yield {'_index': default_index, '_source': record}


async def test():
	'''Test the ingestion process.'''

	async for document in process_data():
		print(document)



if __name__ == '__main__':
	asyncio.run(test())