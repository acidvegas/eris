#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_meshtastic.py

import asyncio
import json
import logging
import time

try:
	import aiofiles
except ImportError:
	raise ImportError('Missing required \'aiofiles\' library. (pip install aiofiles)')


# Set a default elasticsearch index if one is not provided
default_index = 'eris-meshtastic'


def construct_map() -> dict:
	'''Construct the Elasticsearch index mapping for Meshtastic records.'''

	# Match on exact value or full text search
	keyword_mapping = { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

	return {
		'mappings': {
			'properties': {
				'channel' : { 'type': 'long'},
				'decoded' : {
					'properties': {
						'bitfield'       : { 'type': 'long' },
						'payload'        : {
							'type'       : 'nested',
							'dynamic'    : True,
							'properties' : {
								'HDOP'                      : { 'type': 'long' },
								'PDOP'                      : { 'type': 'long' },
								'altitude'                  : { 'type': 'long' },
								'altitudeGeoidalSeparation' : { 'type': 'long' },
								'altitudeHae'               : { 'type': 'long' },
								'deviceMetrics' : {
									'properties' : {
										'airUtilTx'          : { 'type': 'float' },
										'batteryLevel'       : { 'type': 'long'  },
										'channelUtilization' : { 'type': 'float' },
										'uptimeSeconds'      : { 'type': 'long'  },
										'voltage'            : { 'type': 'float' }
									}
								},
								'environmentMetrics' : {
									'properties' : {
										'barometricPressure' : { 'type': 'float' },
										'current'            : { 'type': 'float' },
										'distance'           : keyword_mapping,
										'gasResistance'      : { 'type': 'float' },
										'iaq'                : { 'type': 'long'  },
										'lux'                : { 'type': 'float' },
										'relativeHumidity'   : { 'type': 'float' },
										'temperature'        : { 'type': 'float' },
										'voltage'            : { 'type': 'float' },
										'whiteLux'           : { 'type': 'float' },
										'windDirection'      : { 'type': 'long'  },
										'windSpeed'          : { 'type': 'float' }
									}
								},
								'errorReason'    : keyword_mapping,
								'groundSpeed'    : { 'type': 'long'    },
								'groundTrack'    : { 'type': 'long'    },
								'hwModel'        : keyword_mapping,
								'id'             : keyword_mapping,
								'isLicensed'     : { 'type': 'boolean' },
								'lastSentById'   : { 'type': 'long'    },
								'latitudeI'      : { 'type': 'long'    },
								'locationSource' : keyword_mapping,
								'longName'       : keyword_mapping,
								'longitudeI'     : { 'type': 'long'    },
								'macaddr'        : keyword_mapping,
								'neighbors'      : {
									'properties' : {
										'nodeId' : { 'type': 'long'  },
										'snr'    : { 'type': 'float' }
									}
								},
								'nodeBroadcastIntervalSecs' : { 'type': 'long' },
								'nodeId'                    : { 'type': 'long' },
								'powerMetrics'              : {
									'properties': {
										'ch1Current' : { 'type': 'float' },
										'ch1Voltage' : { 'type': 'float' },
										'ch2Current' : { 'type': 'float' },
										'ch2Voltage' : { 'type': 'float' },
										'ch3Current' : { 'type': 'float' },
										'ch3Voltage' : { 'type': 'float' }
									}
								},
								'precisionBits' : { 'type': 'long' },
								'publicKey'     : keyword_mapping,
								'role'          : keyword_mapping,
								'route'         : { 'type': 'long' },
								'routeBack'     : { 'type': 'long' },
								'satsInView'    : { 'type': 'long' },
								'seqNumber'     : { 'type': 'long' },
								'shortName'     : keyword_mapping,
								'snrBack'       : { 'type': 'long' },
								'snrTowards'    : { 'type': 'long' },
								'time'          : { 'type': 'date' },
								'timestamp'     : { 'type': 'date' }
							}
						},
						'portnum'      : keyword_mapping,
						'requestId'    : { 'type': 'long'    },
						'wantResponse' : { 'type': 'boolean' }
					}
				},
				'from'     : { 'type': 'long'    },
				'hopLimit' : { 'type': 'long'    },
				'hopStart' : { 'type': 'long'    },
				'id'       : { 'type': 'long'    },
				'priority' : keyword_mapping,
				'rxRssi'   : { 'type': 'long'    },
				'rxSnr'    : { 'type': 'float'   },
				'rxTime'   : { 'type': 'date'    },
				'to'       : { 'type': 'long'    },
				'viaMqtt'  : { 'type': 'boolean' },
				'wantAck'  : { 'type': 'boolean' }
			}
		}
	}


async def process_data(input_path: str):
	'''
	Read and process the input file

	:param input_path: Path to the input file
	'''

	async with aiofiles.open(input_path) as input_file:
		async for line in input_file:
			line = line.strip()

			if line == '~eof':
				break

			if not line or not line.startswith('{'):
				continue

			try:
				record = json.loads(line)
			except json.decoder.JSONDecodeError:
				logging.error(f'Failed to parse JSON record! ({line})')
				continue

			# Convert Unix timestamps to Zulu time format
			if 'rxTime' in record:
				record['rxTime'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(record['rxTime']))

			# Handle payload processing
			if 'decoded' in record and 'payload' in record['decoded']:
				payload = record['decoded']['payload']
				
				# If payload is not a dict, wrap it in a nested array with a value field
				if not isinstance(payload, dict):
					record['decoded']['payload'] = [{'value': payload}]
				else:
					# Process timestamps in payload object and ensure it's in an array
					if 'time' in payload:
						payload['time'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(payload['time']))
					if 'timestamp' in payload:
						payload['timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(payload['timestamp']))
					record['decoded']['payload'] = [payload]

			yield {'_index': default_index, '_source': record}


async def test():
	'''Test the ingestion process.'''

	async for document in process_data():
		print(document)



if __name__ == '__main__':
	asyncio.run(test())