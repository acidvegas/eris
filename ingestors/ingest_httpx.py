#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_httpx.py

import json
import logging

try:
	import aiofiles
except ImportError:
	raise ImportError('Missing required \'aiofiles\' library. (pip install aiofiles)')


# Set a default elasticsearch index if one is not provided
default_index = 'eris-httpx'


def construct_map() -> dict:
	'''Construct the Elasticsearch index mapping for Masscan records.'''

	# Match on exact value or full text search
	keyword_mapping = { 'type': 'text',  'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

	# Construct the index mapping
	mapping = {
		'mappings': {
			'properties': {
				'timestamp' : { 'type' : 'date' },
				'hash'      : {
					'properties': {
						'body_md5'       : { 'type': 'keyword' },
						'body_mmh3'      : { 'type': 'keyword' },
						'body_sha256'    : { 'type': 'keyword' },
						'body_simhash'   : { 'type': 'keyword' },
						'header_md5'     : { 'type': 'keyword' },
						'header_mmh3'    : { 'type': 'keyword' },
						'header_sha256'  : { 'type': 'keyword' },
						'header_simhash' : { 'type': 'keyword' }
					}
				},
				'port'               : { 'type': 'integer' },
				'url'                : keyword_mapping,
				'final_url'          : keyword_mapping,
				'input'              : keyword_mapping,
				'title'              : keyword_mapping,
				'scheme'             : { 'type': 'keyword' },
				'webserver'          : { 'type': 'keyword' },
				'body_preview'       : keyword_mapping,
				'content_type'       : { 'type': 'keyword' },
				'method'             : { 'type': 'keyword' },
				'host'               : { 'type': 'ip' },
				'path'               : keyword_mapping,
				'favicon'            : { 'type': 'keyword' },
				'favicon_path'       : keyword_mapping,
				'a'                  : { 'type': 'ip' },
				'cname'              : keyword_mapping,
				'aaaa'               : { 'type': 'ip' },
				'tech'               : keyword_mapping,
				'words'              : { 'type': 'integer' },
				'lines'              : { 'type': 'integer' },
				'status_code'        : { 'type': 'integer' },
				'chain_status_codes' : { 'type': 'integer' },
				'content_length'     : { 'type': 'integer' }
			}
		}
	}

	return mapping


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

			# Skip empty lines
			if not line:
				continue

			# Parse the JSON record
			try:
				record = json.loads(line)
			except json.JSONDecodeError:
				logging.error(f'Failed to parse JSON record: {line}')
				continue

		# Hacky solution to maintain ISO 8601 format without milliseconds or offsets
		record['timestamp'] = record['timestamp'].split('.')[0] + 'Z'

		# Remove unnecessary fields we don't care about
		for item in ('failed', 'knowledgebase', 'time', 'csp'):
			if item in record:
				del record[item]

		yield {'_index': default_index, '_source': record}


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
Deploy:
	go install -v github.com/projectdiscovery/httpx/cmd/httpx@latest 
	curl -s https://public-dns.info/nameservers.txt -o nameservers.txt
	httpx -l fulldomains.txt -t 200 -sc -location -favicon -title -bp -td -ip -cname -mc 200,201,301,302,303,307,308 -fr -r nameservers.txt -retries 2 -stream -sd -j -o fifo.json -v

Output:
	{
		"timestamp":"2024-01-14T13:08:15.117348474-05:00", # Rename to seen and remove milliseconds and offset
		"hash": { # Do we need all of these ?
			"body_md5"       : "4ae9394eb98233b482508cbda3b33a66",
			"body_mmh3"      : "-4111954",
			"body_sha256"    : "89e06e8374353469c65adb227b158b265641b424fba7ddb2c67eef0c4c1280d3",
			"body_simhash"   : "9814303593401624250",
			"header_md5"     : "980366deb2b2fb5df2ad861fc63e79ce",
			"header_mmh3"    : "-813072798",
			"header_sha256"  : "39aea75ad548e38b635421861641ad1919ed3b103b17a33c41e7ad46516f736d",
			"header_simhash" : "10962523587435277678"
		},
		"port"           : "443",
		"url"            : "https://supernets.org", # Remove this and only use the input field as "domain" maybe
		"input"          : "supernets.org", # rename to domain
		"title"          : "SuperNETs",
		"scheme"         : "https",
		"webserver"      : "nginx",
		"body_preview"   : "SUPERNETS Home About Contact Donate Docs Network IRC Git Invidious Jitsi LibreX Mastodon Matrix Sup",
		"content_type"   : "text/html",
		"method"         : "GET", # Remove this
		"host"           : "51.89.151.158",
		"path"           : "/",
		"favicon"        : "-674048714",
		"favicon_path"   : "/i/favicon.png",
		"time"           : "592.907689ms", # Do we need this ?
		"a"              : ["6.150.220.23"],
		"tech"           : ["Bootstrap:4.0.0", "HSTS", "Nginx"],
		"words"          : 436, # Do we need this ?
		"lines"          : 79, # Do we need this ?
		"status_code"    : 200,
		"content_length" : 4597,
		"failed"         : false, # Do we need this ?
		"knowledgebase"  : { # Do we need this ?
			"PageType" : "nonerror",
			"pHash"    : 0
		}
	}
'''
