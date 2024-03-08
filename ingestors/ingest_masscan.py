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


default_index = 'masscan-logs'


def construct_map() -> dict:
    '''Construct the Elasticsearch index mapping for Masscan records.'''

    keyword_mapping = { 'type': 'text',  'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

    geoip_mapping = {
        'city_name'        : keyword_mapping,
        'continent_name'   : keyword_mapping,
        'country_iso_code' : keyword_mapping,
        'country_name'     : keyword_mapping,
        'location'         : { 'type': 'geo_point' },
        'region_iso_code'  : keyword_mapping,
        'region_name'      : keyword_mapping,
    }

    mapping = {
        'mappings': {
            'properties': {
                'ip'      : { 'type': 'ip' },
                'port'    : { 'type': 'integer' },
                'data'    : {
                    'properties': {
                        'proto'   : { 'type': 'keyword' },
                        'service' : { 'type': 'keyword' },
                        'banner'  : keyword_mapping,
                        'seen'    : { 'type': 'date' }
                    }
                },
                #'geoip'    : { 'properties': geoip_mapping } # Used with the geoip pipeline to enrich the data
                'last_seen' : { 'type': 'date' }                
            }
        }
    }

    return mapping


async def process_data(file_path: str):
    '''
    Read and process Masscan records from the log file.

    :param file_path: Path to the Masscan log file
    '''

    async with aiofiles.open(file_path, mode='r') as input_file:
        async for line in input_file:
            line = line.strip()

            if line == '~eof': # Sentinel value to indicate the end of a process (Used with --watch with FIFO)
                break

            if not line or not line.startswith('{'):
                continue

            if line.endswith(','): # Do we need this? Masscan JSON output seems with seperate records with a comma between lines for some reason...
                line = line[:-1]

            try:
                record = json.loads(line)
            except json.decoder.JSONDecodeError:
                # In rare cases, the JSON record may be incomplete or malformed:
                #   { "ip": "51.161.12.223", "timestamp": "1707628302", "ports": [ {"port": 22, "proto": "tcp", "service": {"name": "ssh", "banner":
                #   { "ip": "83.66.211.246", "timestamp": "1706557002"
                logging.error(f'Failed to parse JSON record! ({line})')
                input('Press Enter to continue...') # Pause for review & debugging (remove this in production)
                continue

            if len(record['ports']) > 1:
                # In rare cases, a single record may contain multiple ports, though I have yet to witness this...
                logging.warning(f'Multiple ports found for record! ({record})')
                input('Press Enter to continue...') # Pause for review (remove this in production)

            for port_info in record['ports']:
                struct = {
                    'ip'   : record['ip'],
                    'data' : {
                        'port'  : port_info['port'],
                        'proto' : port_info['proto'],
                        'seen'  : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(int(record['timestamp']))),
                    },
                    'last_seen' : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(int(record['timestamp']))),
                }

                if 'service' in port_info:
                    if 'name' in port_info['service']:
                        if (service_name := port_info['service']['name']) not in ('unknown',''):
                            struct['service'] = service_name

                    if 'banner' in port_info['service']:
                        banner = ' '.join(port_info['service']['banner'].split()) # Remove extra whitespace
                        if banner:
                            struct['banner'] = banner

                id = f'{record["ip"]}:{port_info["port"]}' # Store with ip:port as the unique id to allow the record to be reindexed if it exists.

                yield {'_id': id, '_index': default_index, '_source': struct}



'''
Example record:
{
    "ip"        : "43.134.51.142",
    "timestamp" : "1705255468", # Convert to ZULU BABY
    "ports"     : [ # We will create a record for each port opened
        {
            "port"    : 22,
            "proto"   : "tcp",
            "service" : { # This field is optional
                "name"   : "ssh",
                "banner" : "SSH-2.0-OpenSSH_8.9p1 Ubuntu-3ubuntu0.4"
            }
        }
    ]
}

Will be indexed as:
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



'''
Notes:

apt-get install iptables masscan libpcap-dev screen
setcap 'CAP_NET_RAW+eip CAP_NET_ADMIN+eip' /bin/masscan
/sbin/iptables -A INPUT -p tcp --dport 61010 -j DROP # Not persistent
printf "0.0.0.0/8\n10.0.0.0/8\n100.64.0.0/10\n127.0.0.0/8\n169.254.0.0/16\n172.16.0.0/12\n192.0.0.0/24\n192.0.2.0/24\n192.31.196.0/24\n192.52.193.0/24\n192.88.99.0/24\n192.168.0.0/16\n192.175.48.0/24\n198.18.0.0/15\n198.51.100.0/24\n203.0.113.0/24\n224.0.0.0/3\n255.255.255.255/32"  > exclude.conf
screen -S scan
masscan 0.0.0.0/0 -p21,22,23 --banners --http-user-agent "USER_AGENT" --source-port 61010 --open-only --rate 30000 --excludefile exclude.conf -oJ output.json
masscan 0.0.0.0/0 -p21,22,23 --banners --http-user-agent "USER_AGENT" --source-port 61000-65503 --open-only --rate 30000 --excludefile exclude.conf -oJ output_new.json --shard $i/$TOTAL
'''