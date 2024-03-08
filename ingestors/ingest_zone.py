#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_zone.py

import logging
import time

try:
    import aiofiles
except ImportError:
    raise ImportError('Missing required \'aiofiles\' library. (pip install aiofiles)')


default_index = 'dns-zones'
record_types  = ('a','aaaa','caa','cdnskey','cds','cname','dnskey','ds','mx','naptr','ns','nsec','nsec3','nsec3param','ptr','rrsig','rp','sshfp','soa','srv','txt','type65534')


def construct_map() -> dict:
    '''Construct the Elasticsearch index mapping for zone file records.'''

    keyword_mapping = { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

    mapping = {
        'mappings': {
            'properties': {
                'domain'  : keyword_mapping,
                'records' : { 'properties': {} },
                'seen'    : { 'type': 'date' }
            }
        }
    }

    # Add record types to mapping dynamically to not clutter the code
    for item in record_types:
        if item in ('a','aaaa'):
            mapping['mappings']['properties']['records']['properties'][item] = {
                'properties': {
                    'data': { 'type': 'ip' },
                    'ttl':  { 'type': 'integer' }
                }
            }
        else:
            mapping['mappings']['properties']['records']['properties'][item] = {
                'properties': {
                'data': keyword_mapping,
                'ttl':  { 'type': 'integer' }
                }
            }

    return mapping


async def process_data(file_path: str):
    '''
    Read and process zone file records.

    :param file_path: Path to the zone file
    '''

    async with aiofiles.open(file_path) as input_file:

        last = None

        async for line in input_file:
            line = line.strip()

            if line == '~eof': # Sentinel value to indicate the end of a process (Used with --watch with FIFO)
                return last

            if not line or line.startswith(';'):
                continue

            parts = line.split()

            if len(parts) < 5:
                logging.warning(f'Invalid line: {line}')

            domain, ttl, record_class, record_type, data = parts[0].rstrip('.').lower(), parts[1], parts[2].lower(), parts[3].lower(), ' '.join(parts[4:])

            if not ttl.isdigit():
                logging.warning(f'Invalid TTL: {ttl} with line: {line}')
                continue
            
            ttl = int(ttl)

            # Anomaly...Doubtful any CHAOS/HESIOD records will be found in zone files
            if record_class != 'in':
                logging.warning(f'Unsupported record class: {record_class} with line: {line}')
                continue

            # We do not want to collide with our current mapping (Again, this is an anomaly)
            if record_type not in record_types:
                logging.warning(f'Unsupported record type: {record_type} with line: {line}')
                continue

            # Little tidying up for specific record types (removing trailing dots, etc)
            if record_type == 'nsec':
                data = ' '.join([data.split()[0].rstrip('.'), *data.split()[1:]])
            elif record_type == 'soa':
                data = ' '.join([part.rstrip('.') if '.' in part else part for part in data.split()])
            elif data.endswith('.'):
                data = data.rstrip('.')

            if last:
                if domain == last['domain']:
                    if record_type in last['_doc']['records']:
                        last['_doc']['records'][record_type].append({'ttl': ttl, 'data': data}) # Do we need to check for duplicate records?
                    else:
                        last['_doc']['records'][record_type] = [{'ttl': ttl, 'data': data}]
                    continue
                else:
                    yield last

            last = {
                '_op_type' : 'update',
                '_id'      : domain,
                '_index'   : default_index,
                '_doc'     : {
                    'domain'  : domain,
                    'records' : {record_type: [{'ttl': ttl, 'data': data}]},
                    'seen'    : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) # Zone files do not contain a timestamp, so we use the current time
                },
                'doc_as_upsert' : True # This will create the document if it does not exist
            }


async def test(input_path: str):
    '''
    Test the Zone file ingestion process
    
    :param input_path: Path to the MassDNS log file
    '''
    async for document in process_data(input_path):
        print(document)



if __name__ == '__main__':
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description='Zone file Ingestor for ERIS')
    parser.add_argument('input_path', help='Path to the input file or directory')
    args = parser.parse_args()
    
    asyncio.run(test(args.input_path))



'''
Output:
    1001.vegas. 3600 in ns ns11.waterrockdigital.com.
    1001.vegas. 3600 in ns ns12.waterrockdigital.com.

Input:
    {
        '_id'     : '1001.vegas'
        '_index'  : 'dns-zones',
        '_source' : {
            'domain'  : '1001.vegas',        
            'records' : {
                'ns': [
                    {'ttl': 3600, 'data': 'ns11.waterrockdigital.com'},
                    {'ttl': 3600, 'data': 'ns12.waterrockdigital.com'}
                ]
            },
            'seen'    : '2021-09-01T00:00:00Z'
        }
    }

Notes:
    How do we want to handle hashed NSEC3 records? Do we ignest them as they are, or crack the NSEC3 hashes first and ingest?
'''