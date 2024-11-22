#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_massdns.py

import logging
import time

try:
	import aiofiles
except ImportError:
	raise ImportError('Missing required \'aiofiles\' library. (pip install aiofiles)')


# Set a default elasticsearch index if one is not provided
default_index = 'eris-massdns'


def construct_map() -> dict:
	'''Construct the Elasticsearch index mapping for records'''

	# Match on exact value or full text search
	keyword_mapping = { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

	# Construct the index mapping
	mapping = {
		'mappings': {
			'properties': {
				'ip'     : { 'type': 'ip' },
				'record' : keyword_mapping,
				'seen'   : { 'type': 'date' }
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

		# Cache the last document to avoid creating a new one for the same IP address
		last = None

		try:
			# Read the input file line by line
			async for line in input_file:
				line = line.strip()

				# Sentinel value to indicate the end of a process (for closing out a FIFO stream)
				if line == '~eof':
					yield last
					break

				# Skip empty lines (doubtful we will have any, but just in case)
				if not line:
					continue

				# Split the line into its parts
				parts = line.split()

				# Ensure the line has at least 3 parts
				if len(parts) < 3:
					logging.warning(f'Invalid PTR record: {line}')
					continue

				# Split the PTR record into its parts
				name, record_type, record = parts[0].rstrip('.'), parts[1], ' '.join(parts[2:]).rstrip('.')

				# Do not index other records
				if record_type != 'PTR':
					continue

				# Do not index PTR records that do not have a record
				if not record:
					continue

				# Do not index PTR records that have the same record as the in-addr.arpa domain
				if record == name:
					continue

				# Get the IP address from the in-addr.arpa domain
				ip = '.'.join(name.replace('.in-addr.arpa', '').split('.')[::-1])

				# Check if we are still processing the same IP address
				if last:
					if ip == last['_id']: # This record is for the same IP address as the cached document
						last_records = last['doc']['record']
						if record not in last_records: # Do not index duplicate records
							last['doc']['record'].append(record)
						continue
					else:
						yield last # Return the last document and start a new one

				# Cache the document
				last = {
					'_op_type' : 'update',
					'_id'      : ip,
					'_index'   : default_index,
					'doc'      : {
						'ip'     : ip,
						'record' : [record],
						'seen'   : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
					},
					'doc_as_upsert' : True # Create the document if it does not exist
				}

		except Exception as e:
			logging.error(f'Error processing data: {e}')


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
Deployment:
	printf "\nsession    required   pam_limits.so" >> /etc/pam.d/su
	printf "acidvegas    hard    nofile    65535\nacidvegas    soft    nofile    65535" >> /etc/security/limits.conf
	echo "net.netfilter.nf_conntrack_max = 131072" >> /etc/sysctl.conf
	echo "net.netfilter.nf_conntrack_udp_timeout = 30" >> /etc/sysctl.conf
	echo "net.netfilter.nf_conntrack_udp_timeout_stream = 120" >> /etc/sysctl.conf
	echo "net.netfilter.nf_conntrack_tcp_timeout_established = 300" >> /etc/sysctl.conf
	sysctl -p

	sudo apt-get install build-essential gcc make python3 python3-pip
	pip install aiofiles aiohttp elasticsearch
	git clone --depth 1 https://github.com/acidvegas/eris.git $HOME/eris

	git clone --depth 1 https://github.com/blechschmidt/massdns.git $HOME/massdns && cd $HOME/massdns && make
	wget -O $HOME/massdns/resolvers.txt https://raw.githubusercontent.com/trickest/resolvers/refs/heads/main/resolvers.txt
	while true; do python3 ./scripts/ptr.py | ./bin/massdns -r $HOME/massdns/resolvers.txt -t PTR --filter NOERROR -s 5000 -o S -w $HOME/eris/FIFO; done

	screen -S eris
	python3 $HOME/eris/eris.py --massdns



Output:
	0.6.229.47.in-addr.arpa. PTR 047-229-006-000.res.spectrum.com.
	0.6.228.75.in-addr.arpa. PTR 0.sub-75-228-6.myvzw.com.
	0.6.207.73.in-addr.arpa. PTR c-73-207-6-0.hsd1.ga.comcast.net.


Input:
	{
		'_id'     : '47.229.6.0'
		'_index'  : 'eris-massdns',
		'_source' : {
			'ip'     : '47.229.6.0',
			'record' : ['047-229-006-000.res.spectrum.com'], # We will store as a list for IP addresses with multiple PTR records
			'seen'   : '2021-06-30T18:31:00Z'
		}
	}


Notes:
	Why do some IP addresses return a A/CNAME from a PTR request
	What is dns-servfail.net (Frequent CNAME response from PTR requests)
'''
