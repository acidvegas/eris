#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_masscan.py

import json
import logging
import re
import time

default_index = 'masscan-logs'

def construct_map() -> dict:
    '''Construct the Elasticsearch index mapping for Masscan records.'''

    keyword_mapping = { 'type': 'text',  'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

    mapping = {
        'mappings': {
            'properties': {
                'ip':      { 'type': 'ip' },
                'port':    { 'type': 'integer' },
                'proto':   { 'type': 'keyword' },
                'service': { 'type': 'keyword' },
                'banner':  keyword_mapping,
                'ref_id':  { 'type': 'keyword' },
                'seen':    { 'type': 'date' },
                'geoip':   {
                    'properties': {
                        'city_name':        keyword_mapping,
                        'continent_name':   keyword_mapping,
                        'country_iso_code': keyword_mapping,
                        'country_name':     keyword_mapping,
                        'location':         { 'type': 'geo_point' },
                        'region_iso_code':  keyword_mapping,
                        'region_name':      keyword_mapping,
                    }
                }
            }
        }
    }

    return mapping


def process_file(file_path: str):
    '''
    Read and process Masscan records from the log file.

    :param file_path: Path to the Masscan log file
    '''

    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()

            if not line or not line.startswith('{'):
                continue

            try:
                record = json.loads(line)
            except json.decoder.JSONDecodeError:
                logging.error(f'Failed to parse JSON record! ({line})')
                input('Press Enter to continue...') # Debugging
                continue

            for port_info in record['ports']:
                struct = {
                    'ip': record['ip'],
                    'port': port_info['port'],
                    'proto': port_info['proto'],
                    'seen': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(int(record['timestamp']))),
                }

                if 'service' in port_info:
                    if 'name' in port_info['service']:
                        if port_info['service']['name'] != 'unknown':
                            struct['service'] = port_info['service']['name']

                    if 'banner' in port_info['service']:
                        banner = ' '.join(port_info['service']['banner'].split()) # Remove extra whitespace
                        if banner:
                            match = re.search(r'\(Ref\.Id: (.*?)\)', banner)
                            if match:
                                struct['ref_id'] = match.group(1)
                            else:
                                struct['banner'] = banner

                yield struct
 
    return None # EOF



'''
Example record:
{
    "ip": "43.134.51.142",
    "timestamp": "1705255468", # Convert to ZULU BABY
    "ports": [ # We will create a record for each port opened
        {
            "port": 22,
            "proto": "tcp",
            "service": { # This field is optional
                "name": "ssh",
                "banner": "SSH-2.0-OpenSSH_8.9p1 Ubuntu-3ubuntu0.4"
            }
        }
    ]
}

Will be indexed as:
{
    "ip": "43.134.51.142",
    "port": 22,
    "proto": "tcp",
    "service": "ssh",
    "banner": "SSH-2.0-OpenSSH_8.9p1 Ubuntu-3ubuntu0.4",
    "seen": "2021-10-08T02:04:28Z",
    "ref_id": "?sKfOvsC4M4a2W8PaC4zF?", # TCP RST Payload, Might be useful..

    # GeoIP ingestion pipeline fields
    "geoip": {
        "city_name": "City",
        "continent_name": "Continent",
        "country_iso_code": "CC",
        "country_name": "Country",
        "location": {
            "lat": 0.0000,
            "lon": 0.0000
        },
        "region_iso_code": "RR",
        "region_name": "Region"
    }
}
'''