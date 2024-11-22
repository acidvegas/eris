#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_rir_delegations.py

import csv
import ipaddress
import logging
import time

try:
	import aiohttp
except ImportError:
	raise ImportError('Missing required \'aiohttp\' library. (pip install aiohttp)')


# Set a default elasticsearch index if one is not provided
default_index = 'eris-rir-delegations'

# Delegation data sources
delegation_db = {
	'afrinic' : 'https://ftp.afrinic.net/stats/afrinic/delegated-afrinic-extended-latest',
	'apnic'   : 'https://ftp.apnic.net/stats/apnic/delegated-apnic-extended-latest',
	'arin'    : 'https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest',
	'lacnic'  : 'https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest',
	'ripencc' : 'https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest'
}


def construct_map() -> dict:
	'''Construct the Elasticsearch index mapping for records'''

	# Match on exact value or full text search
	keyword_mapping = { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

	# Construct the index mapping
	mapping = {
		'mappings': {
			'properties': {
				'registry'   : { 'type': 'keyword' },
				'cc'         : { 'type': 'keyword' }, # ISO 3166 2-letter code
				'asn'        : {
        			'properties': {
						'start' : { 'type': 'integer' },
						'end'   : { 'type': 'integer' }
					}
				},
				'ip'         : {
					'properties': {
						'start' : { 'type': 'ip' },
						'end'   : { 'type': 'ip' }
					}
				},
				'date'       : { 'type': 'date'    },
				'status'     : { 'type': 'keyword' },
				'extensions' : keyword_mapping,
				'seen'       : { 'type': 'date' }
			}
		}
	}

	return mapping


async def process_data(place_holder: str = None):
	'''
	Read and process the delegation data.

	:param place_holder: Placeholder parameter to match the process_data function signature of other ingestors.
	'''

	for registry, url in delegation_db.items():
		try:
			headers = {'Connection': 'keep-alive'} # This is required for AIOHTTP connections to LACNIC

			async with aiohttp.ClientSession(headers=headers) as session:
				async with session.get(url) as response:
					if response.status != 200:
						logging.error(f'Failed to fetch {registry} delegation data: {response.status}')
						continue

					csv_data   = await response.text()
					rows       = [line.lower() for line in csv_data.split('\n') if line and not line.startswith('#')]
					csv_reader = csv.reader(rows, delimiter='|')

					del rows, csv_data # Cleanup

					# Process the CSV data
					for row in csv_reader:
						cache = '|'.join(row) # Cache the last row for error handling

						# Heuristic for the header line (not doing anything with it for now)
						if len(row) == 7 and row[1] != '*':
							header = {
								'version'   : row[0],
								'registry'  : row[1],
								'serial'    : row[2],
								'records'   : row[3],
								'startdate' : row[4],
								'enddate'   : row[5],
								'UTCoffset' : row[6]
							}
							continue

						# Heuristic for the summary lines (not doing anything with it for now)
						elif row[2] != '*' and row[3] == '*':
							summary = {
								'registry' : row[0],
								'type'     : row[2],
								'count'    : row[4]
							}
							continue

						# Record lines (this is what we want)
						else:
							record = {
								'registry' : row[0],
								'cc'       : row[1],
								'type'     : row[2],
								'start'    : row[3],
								'value'    : row[4],
								'date'     : row[5],
								'status'   : row[6]
							}

							if len(row) == 7:
								if row[7]:
									record['extensions'] = row[7]

							if not record['cc']:
								del record['cc']
							elif len(record['cc']) != 2:
								raise ValueError(f'Invalid country code: {cache}')

							if not record['value'].isdigit():
								raise ValueError(f'Invalid value: {cache}')

							if record['type'] == 'asn':
								end = int(record['start']) + int(record['value']) - 1
								record['asn'] = { 'start': int(record['start']), 'end': end }
							elif record['type'] in ('ipv4', 'ipv6'):
								try:
									if record['type'] == 'ipv4':
										end = ipaddress.ip_address(record['start']) + int(record['value']) - 1
									elif record['type'] == 'ipv6':
										end = ipaddress.ip_network(f'{record["start"]}/{record["value"]}').broadcast_address
										end = end.compressed.lower()
									record['ip'] = { 'start': record['start'], 'end': str(end) }
								except ValueError:
									raise ValueError(f'Invalid IP range: {cache}')
							else:
								raise ValueError(f'Invalid record type: {cache}')

							del record['start'], record['value'], record['type'] # Cleanup variables no longer needed

							if not record['date'] or record['date'] == '00000000':
								del record['date']
							else:
								record['date'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(record['date'], '%Y%m%d'))

							if record['status'] not in ('allocated', 'assigned', 'available', 'reserved', 'unallocated', 'unknown'):
								raise ValueError(f'Invalid status: {cache}')
							
							# Set the seen timestamp
							record['seen'] = time.strftime('%Y-%m-%dT%H:%M:%SZ')

							# Let's just index the records themself (for now)
							yield {'_index': default_index, '_source': record}

		except Exception as e:
			logging.error(f'Error processing {registry} delegation data: {e}')


async def test():
	'''Test the ingestion process'''

	async for document in process_data():
		print(document)



if __name__ == '__main__':
	import asyncio

	asyncio.run(test())



'''
Output:
	arin|US|ipv4|76.15.132.0|1024|20070502|allocated|6c065d5b54b877781f05e7d30ebfff28
	arin|US|ipv4|76.15.136.0|2048|20070502|allocated|6c065d5b54b877781f05e7d30ebfff28
	arin|US|ipv4|76.15.144.0|4096|20070502|allocated|6c065d5b54b877781f05e7d30ebfff28
	arin|US|ipv4|76.15.160.0|8192|20070502|allocated|6c065d5b54b877781f05e7d30ebfff28

Input:
	{
		'registry'   : 'arin',
		'cc'         : 'us',
		'type'       : 'ipv4',
		'ip'         : { 'start': '76.15.132.0', 'end': '76.16.146.0' },
		'date'       : '2007-05-02T00:00:00Z',
		'status'     : 'allocated',
		'extensions' : '6c065d5b54b877781f05e7d30ebfff28'
	}

Notes:
	Do we make this handle the database locally or load it into ram?
'''
