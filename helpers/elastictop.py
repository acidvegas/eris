#!/usr/bin/env python
# estop - developed by acidvegas (https://git.acid.vegas/eris)

'''
Little script to show some basic information about an Elasticsearch cluster.
'''

import argparse
import os

try:
    from elasticsearch import Elasticsearch
except ImportError:
    raise ImportError('Missing required \'elasticsearch\' library. (pip install elasticsearch)')


def bytes_to_human_readable(num_bytes):
    '''Convert bytes to a human-readable format.'''
    for unit in ['bytes', 'kb', 'mb', 'gb', 'tb', 'pb', 'eb', 'zb']:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:3.1f}{unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f}YB"

def main():
    '''Main function when running this script directly.'''

    parser = argparse.ArgumentParser(description='Index data into Elasticsearch.')
    parser.add_argument('--host', default='localhost', help='Elasticsearch host')
    parser.add_argument('--port', type=int, default=9200, help='Elasticsearch port')
    parser.add_argument('--user', default='elastic', help='Elasticsearch username')
    parser.add_argument('--password', default=os.getenv('ES_PASSWORD'), help='Elasticsearch password (if not provided, check environment variable ES_PASSWORD)')
    parser.add_argument('--api-key', help='Elasticsearch API Key for authentication')
    parser.add_argument('--self-signed', action='store_false', help='Elasticsearch is using self-signed certificates')
    args = parser.parse_args()

    es_config = {
        'hosts': [f'{args.host}:{args.port}'],
        'verify_certs': args.self_signed,
        'ssl_show_warn': args.self_signed,
        'basic_auth': (args.user, args.password)
    }
    es = Elasticsearch(**es_config)

    while True:
        os.system('clear')

        stats = es.cluster.stats()

        name = stats['cluster_name']
        status = stats['status']
        indices = {
            'total': stats['indices']['count'],
            'shards': stats['indices']['shards']['total'],
            'docs': stats['indices']['docs']['count'],
            'size': bytes_to_human_readable(stats['indices']['store']['size_in_bytes'])
        }
        nodes = {
            'total': stats['_nodes']['total'],
            'successful': stats['_nodes']['successful'],
            'failed': stats['_nodes']['failed']
        }

        if status == 'green':
            print(f'Cluster   {name} (\033[92m{status}\033[0m)')
        elif status == 'yellow':
            print(f'Cluster   {name} (\033[93m{status}\033[0m)')
        elif status == 'red':
            print(f'Cluster   {name} (\033[91m{status}\033[0m)')
        
        print(f'\nNodes     {nodes["total"]} Total, {nodes["successful"]} Successful, {nodes["failed"]} Failed')

        nodes_info = es.nodes.info()

        for node_id, node_info in nodes_info['nodes'].items():
            node_name = node_info['name']
            transport_address = node_info['transport_address']
            #node_stats = es.nodes.stats(node_id=node_id)
            version = node_info['version']
            memory = bytes_to_human_readable(int(node_info['settings']['node']['attr']['ml']['machine_memory']))
            print(f"          {node_name.ljust(7)} | Host: {transport_address.rjust(21)} | Version: {version.ljust(7)} | Processors: {node_info['os']['available_processors']} | Memory: {memory}")

        indices_stats = es.cat.indices(format="json")
        
        print(f'\nIndices   {indices["total"]:,} Total {indices["shards"]:,}, Shards')
        for index in indices_stats:
            index_name = index['index']
            document_count = f'{int(index['docs.count']):,}'
            store_size = index['store.size']
            number_of_shards = int(index['pri'])  # primary shards
            number_of_replicas = int(index['rep'])  # replicas

            if index_name.startswith('.') or document_count == '0':
                continue

            print(f"          {index_name.ljust(15)} | Documents: {document_count.rjust(15)} | {store_size.rjust(7)} [Shards: {number_of_shards:,}, Replicas: {number_of_replicas:,}]")

        dox = f'{indices["docs"]:,}'

        print(f'\nTotal {dox.rjust(48)} {indices["size"].rjust(9)}')

if __name__ == '__main__':
    main()
