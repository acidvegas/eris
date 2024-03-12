#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_certs.py

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
				'domain' : keyword_mapping,
				'seen'   : { 'type': 'date' }
			}
		}
	}

	return mapping


async def process_data(place_holder: str = None):
	'''
	Read and process Certsream records live from the Websocket stream.

	:param place_holder: Placeholder parameter to match the process_data function signature of other ingestors.
	'''

	while True:
		try:
			async with websockets.connect('wss://certstream.calidog.io') as websocket:
				while True:
					# Read a line from the websocket
					line = await websocket.recv()

					# Parse the JSON record
					try:
						record = json.loads(line)
					except json.decoder.JSONDecodeError:
						logging.error(f'Invalid line from the websocket: {line}')
						continue

					# Grab the unique domains from the record (excluding wildcards)
					domains = record['data']['leaf_cert']['all_domains']
					domains = set([domain[2:] if domain.startswith('*.') else domain for domain in domains])

					# Construct the document
					for domain in domains:
						struct = {
							'domain' : domain,
							'seen'   : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
						}

						yield {'_index': default_index, '_source': struct}

		except websockets.ConnectionClosed:
			logging.error('Connection to Certstream was closed. Attempting to reconnect...')
			await asyncio.sleep(3)

		except Exception as e:
			logging.error(f'An error occurred while processing Certstream records! ({e})')
			break


async def test():
	'''Test the ingestion process.'''

	async for document in process_data():
		print(document)



if __name__ == '__main__':
	import asyncio

	asyncio.run(test())



'''
Output:
	{
		"data": {
			"cert_index": 43061646,
			"cert_link": "https://yeti2025.ct.digicert.com/log/ct/v1/get-entries?start=43061646&end=43061646",
			"leaf_cert": {
				"all_domains": [
					"*.d7zdnegbre53n.amplifyapp.com",
					"d7zdnegbre53n.amplifyapp.com"
				],
				"extensions": {
					"authorityInfoAccess"    : "CA Issuers - URI:http://crt.r2m02.amazontrust.com/r2m02.cer\nOCSP - URI:http://ocsp.r2m02.amazontrust.com\n",
					"authorityKeyIdentifier" : "keyid:C0:31:52:CD:5A:50:C3:82:7C:74:71:CE:CB:E9:9C:F9:7A:EB:82:E2\n",
					"basicConstraints"       : "CA:FALSE",
					"certificatePolicies"    : "Policy: 2.23.140.1.2.1",
					"crlDistributionPoints"  : "Full Name:\n URI:http://crl.r2m02.amazontrust.com/r2m02.crl",
					"ctlPoisonByte"          : true,
					"extendedKeyUsage"       : "TLS Web server authentication, TLS Web client authentication",
					"keyUsage"               : "Digital Signature, Key Encipherment",
					"subjectAltName"         : "DNS:d7zdnegbre53n.amplifyapp.com, DNS:*.d7zdnegbre53n.amplifyapp.com",
					"subjectKeyIdentifier"   : "59:32:78:2A:11:03:62:55:BB:3B:B9:80:24:76:28:90:2E:D1:A4:56"
				},
				"fingerprint": "D9:05:A3:D5:AA:F9:68:BC:0C:0A:15:69:C9:5E:11:92:32:67:4F:FA",
				"issuer": {
					"C"            : "US",
					"CN"           : "Amazon RSA 2048 M02",
					"L"            : null,
					"O"            : "Amazon",
					"OU"           : null,
					"ST"           : null,
					"aggregated"   : "/C=US/CN=Amazon RSA 2048 M02/O=Amazon",
					"emailAddress" : null
				},
				"not_after"           : 1743811199,
				"not_before"          : 1709596800,
				"serial_number"       : "FDB450C1942E3D30A18737063449E62",
				"signature_algorithm" : "sha256, rsa",
				"subject": {
					"C"            : null,
					"CN"           : "*.d7zdnegbre53n.amplifyapp.com",
					"L"            : null,
					"O"            : null,
					"OU"           : null,
					"ST"           : null,
					"aggregated"   : "/CN=*.d7zdnegbre53n.amplifyapp.com",
					"emailAddress" : null
				}
			},
			"seen": 1709651773.594684,
			"source": {
				"name" : "DigiCert Yeti2025 Log",
				"url"  : "https://yeti2025.ct.digicert.com/log/"
			},
			"update_type": "PrecertLogEntry"
		},
		"message_type": "certificate_update"
	}
'''
