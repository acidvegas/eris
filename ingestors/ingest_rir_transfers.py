#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_rir_transfers.py

import json
import ipaddress
import time
from datetime import datetime

try:
	import aiohttp
except ImportError:
	raise ImportError('Missing required \'aiohttp\' library. (pip install aiohttp)')


# Set a default elasticsearch index if one is not provided
default_index = 'eris-rir-transfers'

# Transfers data sources
transfers_db = {
	'afrinic' : 'https://ftp.afrinic.net/stats/afrinic/transfers/transfers_latest.json',
	'apnic'   : 'https://ftp.apnic.net/stats/apnic/transfers/transfers_latest.json',
	'arin'    : 'https://ftp.arin.net/pub/stats/arin/transfers/transfers_latest.json',
	'lacnic'  : 'https://ftp.lacnic.net/pub/stats/lacnic/transfers/transfers_latest.json',
	'ripencc' : 'https://ftp.ripe.net/pub/stats/ripencc/transfers/transfers_latest.json'
}


def construct_map() -> dict:
	'''Construct the Elasticsearch index mapping for records'''

	# Match on exact value or full text search
	keyword_mapping = { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

	# Construct the index mapping
	mapping = {
		'mappings': {
			'properties': {
				'transfer_date' : { 'type': 'date' },
				'source_registration_date': { 'type': 'date' },
				'recipient_registration_date': { 'type': 'date' },
				'ip4nets' : {
					'properties': {
						'original_set': { 'properties': { 'start_address': { 'type': 'ip' }, 'end_address': { 'type': 'ip' } } },
						'transfer_set': { 'properties': { 'start_address': { 'type': 'ip' }, 'end_address': { 'type': 'ip' } } }
					}
				},
				'ip6nets' : {
					'properties': {
						'original_set': { 'properties': { 'start_address': { 'type': 'ip' }, 'end_address': { 'type': 'ip' } } },
						'transfer_set': { 'properties': { 'start_address': { 'type': 'ip' }, 'end_address': { 'type': 'ip' } } }
					}
				},
				'asns' : {
					'properties': {
						'original_set': { 'properties': { 'start': { 'type': 'integer' }, 'end': { 'type': 'integer' } } },
						'transfer_set': { 'properties': { 'start': { 'type': 'integer' }, 'end': { 'type': 'integer' } } }
					}
				},
				'type'                   : { 'type': 'keyword' },
				'source_organization'    : { 'properties': { 'name':  keyword_mapping, 'country_code': { 'type': 'keyword' } } },
				'recipient_organization' : { 'properties': { 'name':  keyword_mapping, 'country_code': { 'type': 'keyword' } } },
				'source_rir'             : { 'type': 'keyword' },
				'recipient_rir'          : { 'type': 'keyword' },
				'seen'                   : { 'type': 'date' }
			}
		}
	}

	return mapping


def normalize_date(date_str: str) -> str:
	'''
	Convert date string to ISO 8601 format

	:param date_str: Date string to convert
	'''

	try:
		# Parse the date with various formats
		for fmt in ('%Y-%m-%d %H:%M:%S.%f%z', '%Y-%m-%d %H:%M:%S%z', '%Y-%m-%d %H:%M:%S'):
			try:
				dt = datetime.strptime(date_str, fmt)
				return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
			except ValueError:
				continue
		return date_str
	except:
		return date_str


async def process_data(place_holder: str = None):
	'''
	Read and process the transfer data.

	:param place_holder: Placeholder parameter to match the process_data function signature of other ingestors.
	'''

	for registry, url in transfers_db.items():
		try:
			headers = {'Connection': 'keep-alive'} # This is required for AIOHTTP connections to LACNIC

			async with aiohttp.ClientSession(headers=headers) as session:
				async with session.get(url) as response:
					if response.status != 200:
						raise Exception(f'Failed to fetch {registry} delegation data: {response.status}')

					data = await response.text()

					try:
						json_data = json.loads(data)
					except json.JSONDecodeError as e:
						raise Exception(f'Failed to parse {registry} delegation data: {e}')

					if 'transfers' not in json_data:
						raise Exception(f'Invalid {registry} delegation data: {json_data}')

					for record in json_data['transfers']:
						record['seen'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

						# Normalize all date fields
						for date_field in ('transfer_date', 'source_registration_date', 'recipient_registration_date'):
							if date_field in record:
								record[date_field] = normalize_date(record[date_field])

						if 'asns' in record:
							for set_type in ('original_set', 'transfer_set'):
								if set_type in record['asns']:
									count = 0
									for set_block in record['asns'][set_type]:
										for option in ('start', 'end'):
											asn = set_block[option]
											if type(asn) != int:
												if not asn.isdigit():
													raise Exception(f'Invalid {set_type} {option} ASN in {registry} data: {asn}')
												else:
													record['asns'][set_type][count][option] = int(asn)
										count += 1


						if 'ip4nets' in record or 'ip6nets' in record:
							for ip_type in ('ip4nets', 'ip6nets'):
								if ip_type in record:
									for set_type in ('original_set', 'transfer_set'):
										if set_type in record[ip_type]:
											count = 0
											for set_block in record[ip_type][set_type]:
												for option in ('start_address', 'end_address'):
													try:
														ipaddress.ip_address(set_block[option])
													except ValueError as e:
														octets = set_block[option].split('.')
														normalized_ip = '.'.join(str(int(octet)) for octet in octets)
														try:
															ipaddress.ip_address(normalized_ip)
															record[ip_type][set_type][count][option] = normalized_ip
														except ValueError as e:
															raise Exception(f'Invalid {set_type} {option} IP in {registry} data: {e}')
												count += 1

						if record['type'] not in ('MERGER_ACQUISITION', 'RESOURCE_TRANSFER'):
							raise Exception(f'Invalid transfer type in {registry} data: {record["type"]}')

						yield {'_index': default_index, '_source': record}

		except Exception as e:
			raise Exception(f'Error processing {registry} delegation data: {e}')


async def test():
	'''Test the ingestion process'''

	async for document in process_data():
		print(document)



if __name__ == '__main__':
	import asyncio

	asyncio.run(test())



'''
Output:
	{
		"transfer_date" : "2017-09-15T19:00:00Z",
	 	"ip4nets"       : {
			"original_set" : [ { "start_address": "94.31.64.0", "end_address": "94.31.127.255" } ],
		 	"transfer_set" : [ { "start_address": "94.31.64.0", "end_address": "94.31.127.255" } ]
		},
		"type"                   : "MERGER_ACQUISITION",
		"source_organization"    : { "name": "Unser Ortsnetz GmbH" },
		"recipient_organization" : {
			"name"         : "Deutsche Glasfaser Wholesale GmbH",
			"country_code" : "DE"
		},
		"source_rir"    : "RIPE NCC",
		"recipient_rir" : "RIPE NCC"
	},
	{
		"transfer_date" : "2017-09-18T19:00:00Z",
	 	"asns"          : {
			"original_set" : [ { "start": 198257, "end": 198257 } ],
			"transfer_set" : [ { "start": 198257, "end": 198257 } ]
		},
		"type"                   : "MERGER_ACQUISITION",
		"source_organization"    : { "name": "CERT PLIX Sp. z o.o." },
		"recipient_organization" : {
			"name"         : "Equinix (Poland) Sp. z o.o.",
			"country_code" : "PL"
		 },
		"source_rir"    : "RIPE NCC",
		"recipient_rir" : "RIPE NCC"
	}

Input:
	Nothing changed from the output for now...
'''
