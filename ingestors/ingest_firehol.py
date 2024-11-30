#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_firehol.py

import ipaddress
import logging
import time
from typing import Dict, List, Set

try:
    import aiohttp
except ImportError:
    raise ImportError('Missing required libraries. (pip install aiohttp)')

# Set a default elasticsearch index if one is not provided
default_index = 'eris-firehol'

# Base URLs for Firehol IP lists
FIREHOL_BASE_URL = 'https://raw.githubusercontent.com/firehol/blocklist-ipsets/master/'
FIREHOL_API_URL  = 'https://api.github.com/repos/firehol/blocklist-ipsets/git/trees/master'


def construct_map() -> dict:
    '''Construct the Elasticsearch index mapping for Firehol records.'''

    # Match on exact value or full text search
    keyword_mapping = {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}

    # Construct the index mapping
    mapping = {
        'mappings': {
            'properties': {
                'ip'           : { 'type': 'ip'       },
                'network'      : { 'type': 'ip_range' },
                'ipset'        : { 'type': 'keyword'  },
                'category'     : { 'type': 'keyword'  },
                'description'  : keyword_mapping,
                'maintainer'   : { 'type': 'keyword'  },
                'source'       : { 'type': 'keyword'  },
                'seen'         : { 'type': 'date'     },
                'last_updated' : { 'type': 'date'     }
            }
        }
    }

    return mapping

async def fetch_ipset(session: aiohttp.ClientSession, ipset_name: str) -> Dict:
    '''
    Fetch and parse a Firehol ipset.

    :param session: aiohttp client session
    :param ipset_name: Name of the ipset to fetch
    :return: Dictionary containing IPs and metadata
    '''
    # Try both .netset and .ipset extensions
    for ext in ['.netset', '.ipset']:
        url = f'{FIREHOL_BASE_URL}{ipset_name}{ext}'
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    continue

                content = await response.text()
                ips = set()
                metadata = {
                    'category': '',
                    'description': '',
                    'maintainer': ''
                }

                for line in content.splitlines():
                    line = line.strip()
                    
                    # Skip empty lines
                    if not line:
                        continue
                        
                    # Parse metadata from comments
                    if line.startswith('#'):
                        lower_line = line.lower()
                        if 'category:' in lower_line:
                            metadata['category'] = line.split('Category:', 1)[1].strip()
                        elif 'description:' in lower_line:
                            metadata['description'] = line.split('Description:', 1)[1].strip()
                        elif 'maintainer:' in lower_line:
                            metadata['maintainer'] = line.split('Maintainer:', 1)[1].strip()
                        continue

                    # Skip invalid lines
                    if not any(c in '0123456789./:' for c in line):
                        continue

                    try:
                        # Validate IP/network
                        if '/' in line:
                            ipaddress.ip_network(line, strict=False)
                        else:
                            ipaddress.ip_address(line)
                        ips.add(line)
                    except ValueError as e:
                        logging.warning(f'Invalid IP/network in {ipset_name}: {line} ({e})')
                        continue

                return {
                    'ips': ips,
                    'metadata': metadata
                }

        except Exception as e:
            logging.error(f'Error fetching {ipset_name}: {e}')
            continue
    
    return None


async def get_all_ipsets(session: aiohttp.ClientSession) -> List[str]:
    '''
    Fetch list of all available ipsets from the Firehol repository.
    
    :param session: aiohttp client session
    :return: List of ipset names
    '''
    try:
        headers = {'Accept': 'application/vnd.github.v3+json'}
        async with session.get(FIREHOL_API_URL, headers=headers) as response:
            if response.status != 200:
                logging.error(f'Failed to fetch ipset list: HTTP {response.status}')
                return []

            data = await response.json()
            ipsets = []
            
            for item in data['tree']:
                filename = item['path']
                # Include only .netset and .ipset files, exclude directories and other files
                if filename.endswith(('.netset', '.ipset')) and not any(x in filename for x in [
                    '_log', '_report', '_latest', '_1d', '_7d', '_30d', '_90d', '_180d', '_360d', '_720d',
                    'README', 'COPYING', 'LICENSE', 'excluded'
                ]):
                    ipsets.append(filename.rsplit('.', 1)[0])
            
            logging.info(f'Found {len(ipsets)} ipsets')
            return ipsets

    except Exception as e:
        logging.error(f'Error fetching ipset list: {e}')
        return []


async def process_data(input_path: str = None):
    '''
    Process Firehol ipsets and yield records for indexing.
    
    :param input_path: Optional path to local file (not used for Firehol ingestion)
    '''

    # Create a client session
    async with aiohttp.ClientSession() as session:
        # Get list of all available ipsets
        ipset_names = await get_all_ipsets(session)
        
        if not ipset_names:
            logging.error('No ipsets found')
            return

        for ipset_name in ipset_names:
            logging.info(f'Fetching {ipset_name}...')
            
            result = await fetch_ipset(session, ipset_name)
            if not result:
                logging.warning(f'Failed to fetch {ipset_name}')
                continue

            ips = result['ips']
            metadata = result['metadata']
            
            timestamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

            for ip in ips:
                document = {
                    'ip'           : ip.split('/')[0] if '/' in ip else ip,
                    'ipset'        : ipset_name,
                    'category'     : metadata['category'],
                    'description'  : metadata['description'],
                    'maintainer'   : metadata['maintainer'],
                    'source'       : 'firehol',
                    'seen'         : timestamp,
                    'last_updated' : timestamp
                }

                if '/' in ip:
                    document['network'] = ip

                yield {'_index': default_index, '_source': document}

async def test():
    '''Test the ingestion process'''

    async for document in process_data():
        print(document)



if __name__ == '__main__':
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test())



'''
Output Example:

{
    "_index": "eris-firehol",
    "_source": {
        "ip"           : "1.2.3.4",
        "network"      : "1.2.3.0/24",
        "ipset"        : "firehol_level1",
        "category"     : "attacks",
        "description"  : "Basic protection against attacks",
        "maintainer"   : "FireHOL team",
        "source"       : "firehol",
        "seen"         : "2024-03-05T12:00:00Z",
        "last_updated" : "2024-03-05T12:00:00Z"
    }
}
''' 