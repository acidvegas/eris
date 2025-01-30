#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_firehol.py

import ipaddress
import logging
import os
import time
import re

try:
    import git
except ImportError:
    raise ImportError('Missing required libraries. (pip install gitpython)')


# Set a default elasticsearch index if one is not provided
default_index = 'eris-firehol'

# Git repository settings
REPO_URL = 'https://github.com/firehol/blocklist-ipsets.git'
REPO_PATH = os.path.join('data', 'firehol-blocklist') # Local path to store the repo

# File suffixes to ignore
IGNORES = ('_1d', '_7d', '_30d', '_90d', '_180d', '_365d', '_730d')


def construct_map() -> dict:
    '''Construct the Elasticsearch index mapping for Firehol records.'''

    mapping = {
        'mappings': {
            'properties': {
                'ip'         : { 'type': 'ip_range' },
                'ipsets'     : { 'type': 'keyword'  },
                'categories' : { 'type': 'keyword'  },
                'seen'       : { 'type': 'date'     },
            }
        }
    }

    return mapping


def update_repo():
    '''Update the repository locally.'''

    # If the repository doesn't exist, clone it
    if not os.path.exists(REPO_PATH):
        logging.info(f'Cloning repository to {REPO_PATH}...')
    
        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(REPO_PATH), exist_ok=True)
        
        # Clone the repository
        git.Repo.clone_from(REPO_URL, REPO_PATH)
    else:
        # If the repository already exists, update it
        repo = git.Repo(REPO_PATH)
        logging.info('Updating repository...')
        repo.remotes.origin.pull()


def stream_ips(file_path: str):
    '''
    Stream IPs from file, skipping comments and validating each IP.
    
    :param file_path: Path to the ipset file.
    '''

    try:
        # Open the file
        with open(file_path) as f:

            # Iterate over each line
            for line in f:

                # Skip comments and empty lines
                line = line.strip()
                if line.startswith('#') or not line:
                    continue
                
                # Validate IP/network
                try:
                    if not '/' in line:
                        line = f'{line}/32'
                    ipaddress.ip_network(line, strict=True)
                except ValueError as e:
                    logging.warning(f'Invalid IP/network in {os.path.basename(file_path)}: {line} ({e})')
                    continue

                # Yield the valid IP/network
                yield line

    except Exception as e:
        logging.error(f'Error streaming IPs from {file_path}: {e}')


async def process_data(input_path = None):
    '''
    Process Firehol ipsets and yield records for indexing.
    
    :param input_path: Placeholder for uniformity
    '''

    # Update the repository
    update_repo()
        
    # Get all files
    files = []
    for filename in os.listdir(REPO_PATH):
        if filename.endswith(('.ipset', '.netset')):
            if any(filename.rsplit('.', 1)[0].endswith(x) for x in IGNORES):
                logging.debug(f'Ignoring {filename} because it ends with {IGNORES}')
                continue
            files.append(os.path.join(REPO_PATH, filename))

    logging.info(f'Processing {len(files)} files...')
    
    # Dictionary to store unique IPs and their metadata
    ip_records = {}
    
    # Process each file
    for file_path in files:
        logging.info(f'Processing {os.path.basename(file_path)}...')

        # Get the ipset name
        ipset_name = os.path.splitext(os.path.basename(file_path))[0]
        
        # Extract category if present
        category = None
        with open(file_path) as f:
            for line in f:
                if match := re.search(r'^#\s*Category\s*:\s*(.+)$', line, re.IGNORECASE):
                    category = match.group(1).strip()
                    break
        
        # Stream IPs from the file
        for ip in stream_ips(file_path):
            # Initialize record if IP not seen before
            if ip not in ip_records:
                ip_records[ip] = {'ip': ip, 'ipsets': set(), 'categories': set()}
            
            # Update arrays
            ip_records[ip]['ipsets'].add(ipset_name)
            if category:
                ip_records[ip]['categories'].add(category)

    # Yield unique records with converted sets to lists
    for ip, record in ip_records.items():
        # Convert sets to lists for JSON serialization
        record['ipsets']     = list(record['ipsets'])
        record['categories'] = list(record['categories'])
        record['seen']       = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        
        # Yield the document with _id set to the IP
        yield {'_index': default_index, '_id': ip, '_source': record}


async def test():
    '''Test the ingestion process'''

    async for document in process_data():
        print(document)



if __name__ == '__main__':
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test())