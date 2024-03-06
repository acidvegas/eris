#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_zone.py

import time

try:
    import aiofiles
except ImportError:
    raise ImportError('Missing required \'aiofiles\' library. (pip install aiofiles)')


default_index = 'dns-zones'
record_types  = ('a','aaaa','caa','cdnskey','cds','cname','dnskey','ds','mx','naptr','ns','nsec','nsec3','nsec3param','ptr','rrsig','rp','sshfp','soa','srv','txt','type65534')


def construct_map() -> dict:
    '''Construct the Elasticsearch index mapping for zone file records.'''

    keyword_mapping = { 'type': 'text',  'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

    mapping = {
        'mappings': {
            'properties': {
                'domain':  keyword_mapping,
                'records': { 'properties': {} },
                'seen':    {'type': 'date'}
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

    domain_records = {}
    last_domain = None

    async with aiofiles.open(file_path, mode='r') as input_file:
        async for line in input_file:
            line = line.strip()

            if line == '~eof': # Sentinel value to indicate the end of a process (Used with --watch with FIFO)
                break

            if not line or line.startswith(';'):
                continue

            parts = line.split()

            if len(parts) < 5:
                raise ValueError(f'Invalid line: {line}')

            domain, ttl, record_class, record_type, data = parts[0].rstrip('.').lower(), parts[1], parts[2].lower(), parts[3].lower(), ' '.join(parts[4:])

            if not ttl.isdigit():
                raise ValueError(f'Invalid TTL: {ttl} with line: {line}')
            
            ttl = int(ttl)

            # Anomaly...Doubtful any CHAOS/HESIOD records will be found in zone files
            if record_class != 'in':
                raise ValueError(f'Unsupported record class: {record_class} with line: {line}')

            # We do not want to collide with our current mapping (Again, this is an anomaly)
            if record_type not in record_types:
                raise ValueError(f'Unsupported record type: {record_type} with line: {line}')

            # Little tidying up for specific record types (removing trailing dots, etc)
            if record_type == 'nsec':
                data = ' '.join([data.split()[0].rstrip('.'), *data.split()[1:]])
            elif record_type == 'soa':
                    data = ' '.join([part.rstrip('.') if '.' in part else part for part in data.split()])
            elif data.endswith('.'):
                data = data.rstrip('.')

            if domain != last_domain:
                if last_domain:
                    struct = {
                        'domain'  : last_domain,
                        'records' : domain_records[last_domain],
                        'seen'    : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) # Zone files do not contain a timestamp, so we use the current time
                    }
                    
                    del domain_records[last_domain]

                    yield {'_id': domain, '_index': default_index, '_source': struct} # Set the ID to the domain name to allow the record to be reindexed if it exists.

                last_domain = domain

                domain_records[domain] = {}

            if record_type not in domain_records[domain]:
                domain_records[domain][record_type] = []

            domain_records[domain][record_type].append({'ttl': ttl, 'data': data})



'''
Example record:
0so9l9nrl425q3tf7dkv1nmv2r3is6vm.vegas. 3600    in  nsec3   1 1 100 332539EE7F95C32A 10MHUKG4FHIAVEFDOTF6NKU5KFCB2J3A NS DS RRSIG
0so9l9nrl425q3tf7dkv1nmv2r3is6vm.vegas. 3600    in  rrsig   NSEC3 8 2 3600 20240122151947 20240101141947 4125 vegas. hzIvQrZIxBSwRWyiHkb5M2W0R3ikNehv884nilkvTt9DaJSDzDUrCtqwQb3jh6+BesByBqfMQK+L2n9c//ZSmD5/iPqxmTPCuYIB9uBV2qSNSNXxCY7uUt5w7hKUS68SLwOSjaQ8GRME9WQJhY6gck0f8TT24enjXXRnQC8QitY=
1-800-flowers.vegas.    3600    in  ns  dns1.cscdns.net.
1-800-flowers.vegas.    3600    in  ns  dns2.cscdns.net.
100.vegas.  3600    in  ns  ns51.domaincontrol.com.
100.vegas.  3600    in  ns  ns52.domaincontrol.com.
1001.vegas. 3600    in  ns  ns11.waterrockdigital.com.
1001.vegas. 3600    in  ns  ns12.waterrockdigital.com.

Will be indexed as:
{
    "_id"     : "1001.vegas"
    "_index"  : "dns-zones",
    "_source" : {
        "domain"  : "1001.vegas",        
        "records" : { # All records are stored in a single dictionary
            "ns": [
                {"ttl": 3600, "data": "ns11.waterrockdigital.com"},
                {"ttl": 3600, "data": "ns12.waterrockdigital.com"}
            ]
        },
        "seen"    : "2021-09-01T00:00:00Z" # Zulu time added upon indexing
    }
}
'''

'''
Notes:
- How do we want to handle hashed NSEC3 records? Do we ignest them as they are, or crack the NSEC3 hashes first and ingest?
'''