#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# sniff_patch.py

# Note:
#   This is a patch for the elasticsearch 8.x client to fix the sniff_* options.
#   This patch is only needed if you use the sniff_* options and only works with basic auth.
#   Call init_elasticsearch() with normal Elasticsearch params.
#
# Source:
#   - https://github.com/elastic/elasticsearch-py/issues/2005#issuecomment-1645641960

import base64

import elasticsearch._async.client as async_client
from elasticsearch.exceptions import SerializationError, ConnectionError


async def init_elasticsearch_async(*args, **kwargs):
    '''
    Initialize the Async Elasticsearch client with the sniff patch.
    
    :param args: Async Elasticsearch positional arguments.
    :param kwargs: Async Elasticsearch keyword arguments.
    '''
    async_client.default_sniff_callback = _override_async_sniff_callback(kwargs['basic_auth'])

    return async_client.AsyncElasticsearch(*args, **kwargs)


def _override_async_sniff_callback(basic_auth):
    '''
    Taken from https://github.com/elastic/elasticsearch-py/blob/8.8/elasticsearch/_sync/client/_base.py#L166
    Completely unmodified except for adding the auth header to the elastic request.
    Allows us to continue using the sniff_* options while this is broken in the library.

    TODO: Remove this when this issue is patched:
        - https://github.com/elastic/elasticsearch-py/issues/2005
    '''
    auth_str = base64.b64encode(':'.join(basic_auth).encode()).decode()
    sniffed_node_callback = async_client._base._default_sniffed_node_callback

    async def modified_async_sniff_callback(transport, sniff_options):
        for _ in transport.node_pool.all():
            try:
                meta, node_infos = await transport.perform_request(
                    'GET',
                    '/_nodes/_all/http',
                    headers={
                        'accept': 'application/vnd.elasticsearch+json; compatible-with=8',
                        'authorization': f'Basic {auth_str}'  # Authorization header
                    },
                    request_timeout=(
                        sniff_options.sniff_timeout
                        if not sniff_options.is_initial_sniff
                        else None
                    ),
                )
            except (SerializationError, ConnectionError):
                continue

            if not 200 <= meta.status <= 299:
                continue

            node_configs = []
            for node_info in node_infos.get('nodes', {}).values():
                address = node_info.get('http', {}).get('publish_address')
                if not address or ':' not in address:
                    continue

                # Processing address for host and port
                if '/' in address:
                    fqdn, ipaddress = address.split('/', 1)
                    host = fqdn
                    _, port_str = ipaddress.rsplit(':', 1)
                    port = int(port_str)
                else:
                    host, port_str = address.rsplit(':', 1)
                    port = int(port_str)

                assert sniffed_node_callback is not None
                sniffed_node = await sniffed_node_callback(
                    node_info, meta.node.replace(host=host, port=port)
                )
                if sniffed_node is None:
                    continue

                node_configs.append(sniffed_node)

            if node_configs:
                return node_configs

        return []

    return modified_async_sniff_callback