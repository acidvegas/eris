#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_masscan.py

import json
import logging
import time

try:
	import aiofiles
except ImportError:
	raise ImportError('Missing required \'aiofiles\' library. (pip install aiofiles)')


# Set a default elasticsearch index if one is not provided
default_index = 'eris-masscan'


def construct_map() -> dict:
	'''Construct the Elasticsearch index mapping for Masscan records.'''

	# Match on exact value or full text search
	keyword_mapping = { 'type': 'text',  'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

	# Construct the geoip mapping (Used with the geoip pipeline to enrich the data)
	geoip_mapping = {
		'city_name'        : keyword_mapping,
		'continent_name'   : keyword_mapping,
		'country_iso_code' : keyword_mapping,
		'country_name'     : keyword_mapping,
		'location'         : { 'type': 'geo_point' },
		'region_iso_code'  : keyword_mapping,
		'region_name'      : keyword_mapping,
	}

	# Construct the index mapping
	mapping = {
		'mappings': {
			'properties': {
				'ip'      : { 'type': 'ip' },
				'port'    : { 'type': 'integer' },
				'proto'   : { 'type': 'keyword' },
				'service' : { 'type': 'keyword' },
				'banner'  : keyword_mapping,
				'seen'    : { 'type': 'date' }
				#'geoip'	: { 'properties': geoip_mapping }
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

			# Skip empty lines and lines that do not start with a JSON object
			if not line or not line.startswith('{'):
				continue

			# Parse the JSON record
			try:
				record = json.loads(line)
			except json.decoder.JSONDecodeError:
				logging.error(f'Failed to parse JSON record! ({line})')
				continue

			# Process the record
			struct = {
				'ip'    : record['ip'],
				'port'  : record['port'],
				'proto' : record['proto'],
				'seen'  : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(int(record['timestamp'])))
			}

			# Add the service information if available (this field is optional)
			if record['rec_type'] == 'banner':
				data = record['data']
				if 'service_name' in data:
					if (service_name := data['service_name']) not in ('unknown', ''):
						struct['service'] = service_name
				if 'banner' in data:
					banner = ' '.join(data['banner'].split()) # Remove extra whitespace
					if banner:
						struct['banner'] = banner

			# Yield the record
			yield {'_index': default_index, '_source': struct}


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
	apt-get install iptables masscan libpcap-dev screen
	setcap 'CAP_NET_RAW+eip CAP_NET_ADMIN+eip' /bin/masscan
	/sbin/iptables -A INPUT -p tcp --dport 61010 -j DROP # Not persistent
	printf "0.0.0.0/8\n10.0.0.0/8\n100.64.0.0/10\n127.0.0.0/8\n169.254.0.0/16\n172.16.0.0/12\n192.0.0.0/24\n192.0.2.0/24\n192.31.196.0/24\n192.52.193.0/24\n192.88.99.0/24\n192.168.0.0/16\n192.175.48.0/24\n198.18.0.0/15\n198.51.100.0/24\n203.0.113.0/24\n224.0.0.0/3\n255.255.255.255/32"  > exclude.conf
	screen -S scan
	masscan 0.0.0.0/0 -p18000 --banners --http-user-agent "USER_AGENT" --source-port 61010 --open-only --rate 30000 --excludefile exclude.conf -oD 18000.json
	masscan 0.0.0.0/0 -p21,22,23 --banners --http-user-agent "USER_AGENT" --source-port 61000-65503 --open-only --rate 30000 --excludefile exclude.conf -oD output_new.json --shard $i/$TOTAL

Output:
	{
		"ip"        : "43.134.51.142",
		"timestamp" : "1705255468",
		"ports"     : [
			{
				"port"    : 22, # We will create a record for each port opened
				"proto"   : "tcp",
				"service" : {
					"name"   : "ssh",
					"banner" : "SSH-2.0-OpenSSH_8.9p1 Ubuntu-3ubuntu0.4"
				}
			}
		]
	}

Input:
	{
		"_id"     : "43.134.51.142:22"
		"_index"  : "masscan-logs",
		"_source" : {
			"ip"      : "43.134.51.142",
			"port"    : 22,
			"proto"   : "tcp",
			"service" : "ssh",
			"banner"  : "SSH-2.0-OpenSSH_8.9p1 Ubuntu-3ubuntu0.4",
			"seen"    : "2021-10-08T02:04:28Z"
	}
'''
