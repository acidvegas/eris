#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_zone.py

import logging
import time

try:
	import aiofiles
except ImportError:
	raise ImportError('Missing required \'aiofiles\' library. (pip install aiofiles)')


# Set a default elasticsearch index if one is not provided
default_index = 'eris-zones'

# Known DNS record types found in zone files
record_types  = ('a','aaaa','caa','cdnskey','cds','cname','dnskey','ds','mx','naptr','ns','nsec','nsec3','nsec3param','ptr','rrsig','rp','sshfp','soa','srv','txt','type65534')


def construct_map() -> dict:
	'''Construct the Elasticsearch index mapping for zone file records.'''

	# Match on exact value or full text search
	keyword_mapping = { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

	# Construct the index mapping
	mapping = {
		'mappings': {
			'properties': {
				'domain'  : keyword_mapping,
				'zone'    : { 'type': 'keyword' },
				'records' : { 'type': 'nested', 'properties': {} },
				'source'  : { 'type': 'keyword' },
				'seen'    : { 'type': 'date' }
			}
		}
	}

	# Add record types to mapping dynamically to not clutter the code
	for record_type in record_types:
		if record_type in ('a','aaaa'):
			mapping['mappings']['properties']['records']['properties'][record_type] = {
				'type'       : 'nested',
				'properties' : {
					'data' : { 'type': 'ip' if record_type in ('a','aaaa') else keyword_mapping },
					'ttl'  : { 'type': 'integer' }
				}
			}

	return mapping


async def process_data(file_path: str):
	'''
	Read and process the input file

	:param input_path: Path to the input file
	'''

	async with aiofiles.open(file_path) as input_file:

		# Initialize the cache
		last = None

		# Default source for the records
		source = 'czds'

		# Determine the zone name from the file path (e.g., /path/to/zones/com.eu.txt -> com.eu zone)
		zone = '.'.join(file_path.split('/')[-1].split('.')[:-1])
		# Note: For now, this is the best way because we are not just ingesting TLD zone files, but entire zones for domains aswell...

		# Read the input file line by line
		async for line in input_file:
			line = line.strip()

			# Sentinel value to indicate the end of a process (for closing out a FIFO stream)
			if line == '~eof':
				yield last
				break

			# Skip empty lines and comments
			if not line:
				continue

			# Skip comments but detect AXFR transfers to change the source)
			if line.startswith(';'):
				if 'DiG' in line and 'AXFR' in line: # Do we need to worry about case sensitivity? How can we store the nameserver aswell?
					source = 'axfr'
				continue

			# Split the line into its parts
			parts = line.split()

			# Ensure the line has at least 3 parts
			if len(parts) < 5:
				logging.warning(f'Invalid line: {line}')
				continue

			# Split the record into its parts
			domain, ttl, record_class, record_type, data = parts[0].rstrip('.').lower(), parts[1], parts[2].lower(), parts[3].lower(), ' '.join(parts[4:])

			# Ensure the TTL is a number
			if not ttl.isdigit():
				logging.warning(f'Invalid TTL: {ttl} with line: {line}')
				continue
			else:
				ttl = int(ttl)

			# Do not index other record classes (doubtful any CHAOS/HESIOD records will be found in zone files)
			if record_class != 'in':
				logging.warning(f'Unsupported record class: {record_class} with line: {line}')
				continue

			# Do not index other record types
			if record_type not in record_types:
				logging.warning(f'Unsupported record type: {record_type} with line: {line}')
				continue

			# Little tidying up for specific record types (removing trailing dots, etc)
			if record_type == 'nsec':
				data = ' '.join([data.split()[0].rstrip('.'), *data.split()[1:]])
			elif record_type == 'soa':
				data = ' '.join([part.rstrip('.') if '.' in part else part for part in data.split()])
			elif data.endswith('.'):
				data = data.rstrip('.')

			# Check if we are still processing the same domain
			if last:
				if domain == last['doc']['domain']:
					if record_type in last['doc']['records']:
						last['doc']['records'][record_type].append({'ttl': ttl, 'data': data}) # Do we need to check for duplicate records?
					else:
						last['doc']['records'][record_type] = [{'ttl': ttl, 'data': data}]
					continue
				else:
					yield last

			# Cache the document
			last = {
				'_op_type' : 'update',
				'_id'      : domain,
				'_index'   : default_index,
				'doc'     : {
					'domain'  : domain,
					'zone'    : zone,
					'records' : {record_type: [{'data': data, 'ttl': ttl}]},
					'source'  : source,
					'seen'    : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) # Zone files do not contain a timestamp, so we use the current time
				},
				'doc_as_upsert' : True # This will create the document if it does not exist
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



'''
Output:
	1001.vegas. 3600 in ns ns11.waterrockdigital.com.
	1001.vegas. 3600 in ns ns12.waterrockdigital.com.

Input:
	{
		'_id'     : '1001.vegas'
		'_index'  : 'dns-zones',
		'_source' : {
			'domain'  : '1001.vegas',
			'zone'    : 'vegas',
			'records' : {
				'ns': [
					{'ttl': 3600, 'data': 'ns11.waterrockdigital.com'},
					{'ttl': 3600, 'data': 'ns12.waterrockdigital.com'}
				]
			},
			'source'  : 'czds',
			'seen'    : '2021-09-01T00:00:00Z'
		}
	}

Notes:
	How do we want to handle hashed NSEC3 records? Do we ignest them as they are, or crack the NSEC3 hashes first and ingest?
	Can an AXFR transfer return data out of order? If so, how do we handle that?
'''
