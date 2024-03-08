#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_massdns.py

'''
Deployment:
    git clone https://github.com/blechschmidt/massdns.git $HOME/massdns && cd $HOME/massdns && make
    curl -s https://public-dns.info/nameservers.txt | grep -v ':' > $HOME/massdns/nameservers.txt
    pythons ./scripts/ptr.py | ./bin/massdns -r $HOME/massdns/nameservers.txt -t PTR --filter NOERROR-s 1000 -o S -w $HOME/massdns/fifo.json
    or...
    while true; do python ./scripts/ptr.py | ./bin/massdns -r $HOME/massdns/nameservers.txt -t PTR --filter NOERROR -s 1000 -o S -w $HOME/massdns/fifo.json; done

Output:
    0.6.229.47.in-addr.arpa. PTR 047-229-006-000.res.spectrum.com.
    0.6.228.75.in-addr.arpa. PTR 0.sub-75-228-6.myvzw.com.
    0.6.207.73.in-addr.arpa. PTR c-73-207-6-0.hsd1.ga.comcast.net.

Input:
    {
        "_id"     : "47.229.6.0"
        "_index"  : "ptr-records",
        "_source" : {
            "ip"     : "47.229.6.0",
            "record" : "047-229-006-000.res.spectrum.com", # This will be a list if there are more than one PTR record
            "seen"   : "2021-06-30T18:31:00Z"
        }
    }

Notes:
- Why do some IP addresses return a CNAME from a PTR request
- What is dns-servfail.net (Frequent CNAME response from PTR requests)
'''

import logging
import time

try:
    import aiofiles
except ImportError:
    raise ImportError('Missing required \'aiofiles\' library. (pip install aiofiles)')


default_index = 'eris-massdns'


def construct_map() -> dict:
    '''Construct the Elasticsearch index mapping for MassDNS records'''

    keyword_mapping = { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

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


async def process_data(file_path: str):
    '''
    Read and process Massdns records from the log file.

    :param file_path: Path to the Massdns log file
    '''

    async with aiofiles.open(file_path, mode='r') as input_file:

        last = None

        async for line in input_file:
            line = line.strip()

            # Sentinel value to indicate the end of a process (for closing out a FIFO stream)
            if line == '~eof':
                yield last

            # Skip empty lines
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
                logging.warning(f'Invalid record type: {record_type}: {line}')
                continue

            # Do not index PTR records that do not have a record
            if not record:
                logging.warning(f'Empty PTR record: {line}')
                continue

            # Let's not index the PTR record if it's the same as the in-addr.arpa domain
            if record == name:
                logging.warning(f'PTR record is the same as the in-addr.arpa domain: {line}')
                continue
            
            # Get the IP address from the in-addr.arpa domain
            ip = '.'.join(name.replace('.in-addr.arpa', '').split('.')[::-1])

            # Check if we are still processing the same IP address
            if last:
                if ip == last['_id']:
                    last_record = last['_doc']['record']
                    if isinstance(last_record, list):
                        if record not in last_record:
                            last['_doc']['record'].append(record)
                        else:
                            logging.warning(f'Duplicate PTR record: {line}')
                    else:
                        if record != last_record:
                            last['_doc']['record'] = [last_record, record] # IP addresses with more than one PTR record will turn into a list
                    continue
                else:
                    yield last
            
            # Cache the this document in-case we have more for the same IP address
            last = {
                '_op_type' : 'update',
                '_id'      : ip,
                '_index'   : default_index,
                '_doc'     : {
                    'ip'     : ip,
                    'record' : record,
                    'seen'   : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                },
                'doc_as_upsert' : True # This will create the document if it does not exist
            }


async def test(input_path: str):
    '''
    Test the MassDNS ingestion process
    
    :param input_path: Path to the MassDNS log file
    '''
    async for document in process_data(input_path):
        print(document)



if __name__ == '__main__':
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description='MassDNS Ingestor for ERIS')
    parser.add_argument('input_path', help='Path to the input file or directory')
    args = parser.parse_args()
    
    asyncio.run(test(args.input_path))