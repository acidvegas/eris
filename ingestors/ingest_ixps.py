#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_ixps.py

import json
import ipaddress
import time

try:
	import aiohttp
except ImportError:
	raise ImportError('Missing required \'aiohttp\' library. (pip install aiohttp)')


# Set a default elasticsearch index if one is not provided
default_index = 'ixp-' + time.strftime('%Y-%m-%d')


def construct_map() -> dict:
	'''Construct the Elasticsearch index mapping for records'''

	# Match on exact value or full text search
	keyword_mapping = { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

	# Construct the index mapping
	mapping = {
		'mappings': {
			'properties': {
				'name'           : { 'type': 'keyword' },
				'alternatenames' : { 'type': 'keyword' },
				'sources'        : { 'type': 'keyword' },
				'prefixes'       : {
        			'properties': {
               			'ipv4' : { 'type': 'ip'       },
                  		'ipv6' : { 'type': 'ip_range' }
                    }
           		},
				'url'            : { 'type': 'keyword' },
				'region'         : { 'type': 'keyword' },
				'country'        : { 'type': 'keyword' },
				'city'           : { 'type': 'keyword' },
				'state'          : { 'type': 'keyword' },
				'zip'            : { 'type': 'keyword' },
				'address'        : keyword_mapping,
				'iata'           : { 'type': 'keyword' },
				'latitude'       : { 'type': 'float'   },
				'longitude'      : { 'type': 'float'   },
				'geo_id'         : { 'type': 'integer' },
				'ix_id'          : { 'type': 'integer' },
				'org_id'         : { 'type': 'integer' },
				'pdb_id'         : { 'type': 'integer' },
				'pdb_org_id'     : { 'type': 'integer' },
				'pch_id'         : { 'type': 'integer' }
			}
		}
	}

	return mapping


async def process_data():
	'''Read and process the transfers data.'''

	try:
		async with aiohttp.ClientSession() as session:
			async with session.get('https://publicdata.caida.org/datasets/ixps/ixs_202401.jsonl') as response:
				if response.status != 200:
					raise Exception(f'Failed to fetch IXP data: {response.status}')

				data = await response.text()

				try:
					json_data = json.loads(data)
				except json.JSONDecodeError as e:
					raise Exception(f'Failed to parse IXP data: {e}')

				pass

	except Exception as e:
		raise Exception(f'Error processing IXP data: {e}')


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
	"pch_id"         : 1848,
	"name"           : "ANGONIX",
	"country"        : "AO",
	"region"         : "Africa",
	"city"           : "Luanda",
	"iata"           : "LAD",
	"alternatenames" : [],
	"sources"        : ["pch"],
	"prefixes"       : {
		"ipv4" : ["196.11.234.0/24"],
		"ipv6" : ["2001:43f8:9d0::/48"]
	},
	"geo_id" : 2240449,
	"ix_id"  : 10
}
'''
