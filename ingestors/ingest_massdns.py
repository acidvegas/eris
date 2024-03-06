#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_massdns.py

import logging
import time

try:
    import aiofiles
except ImportError:
    raise ImportError('Missing required \'aiofiles\' library. (pip install aiofiles)')

default_index = 'ptr-records'

def construct_map() -> dict:
    '''Construct the Elasticsearch index mapping for MassDNS records'''

    keyword_mapping = { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

    mapping = {
        'mappings': {
                'properties': {
                    'ip'     : { 'type': 'ip' },
                    'name'   : { 'type': 'keyword' },
                    'record' : keyword_mapping,
                    'seen'   : { 'type': 'date' }
                }
            }
        }

    return mapping


async def process_data(file_path: str):
    '''
    Read and process Massdns records from the log file.

    :param file_path: Path to the Massdns log file
    '''

    async with aiofiles.open(file_path, mode='r') as input_file:
        async for line in input_file:
            line = line.strip()

            if line == '~eof': # Sentinel value to indicate the end of a process (Used with --watch with FIFO)
                break

            if not line:
                continue

            parts = line.split()

            if len(parts) < 3:
                raise ValueError(f'Invalid PTR record: {line}')
            
            name, record_type, record = parts[0].rstrip('.'), parts[1], ' '.join(parts[2:]).rstrip('.')

            # Do we handle CNAME records returned by MassDNS?
            if record_type != 'PTR':
                continue

            # Let's not index the PTR record if it's the same as the in-addr.arpa domain
            if record == name:
                continue

            if not record: # Skip empty records
                continue
                    
            ip = '.'.join(name.replace('.in-addr.arpa', '').split('.')[::-1])
            
            struct = {
                'ip'     : ip,
                'record' : record,
                'seen'   : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            }

            yield {'_id': ip, '_index': default_index, '_source': struct} # Store with ip as the unique id to allow the record to be reindexed if it exists.



'''
Example PTR record:
0.6.229.47.in-addr.arpa.  PTR 047-229-006-000.res.spectrum.com.
0.6.228.75.in-addr.arpa.  PTR 0.sub-75-228-6.myvzw.com.
0.6.207.73.in-addr.arpa.  PTR c-73-207-6-0.hsd1.ga.comcast.net.
0.6.212.173.in-addr.arpa. PTR 173-212-6-0.cpe.surry.net.
0.6.201.133.in-addr.arpa. PTR flh2-133-201-6-0.tky.mesh.ad.jp.

Will be indexed as:
{
    "_id"     : "47.229.6.0"
    "_index"  : "ptr-records",
    "_source" : {
        "ip"     : "47.229.6.0",
        "record" : "047-229-006-000.res.spectrum.com.",
        "seen"   : "2021-06-30T18:31:00Z"
    }
}
'''