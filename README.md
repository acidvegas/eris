# Elasticsearch Recon Ingestion Scripts (ERIS)
> A utility for ingesting various large scale reconnaissance data logs into Elasticsearch

The is a suite of tools to aid in the ingestion of recon data from various sources *(httpx, masscan, zonefiles, etc)* into an [Elasticsearch](https://www.elastic.co/elasticsearch) cluster. The entire codebase is designed with asynconous processing, aswell as load balancing ingestion across all of the nodes in your cluster. Additionally, live data ingestion is supported from many of the sources supported. This means data can be directly processed and ingested into your Elasticsearch cluster instantly. The structure allows for the developement of "modules" or "plugins" if you will, to quickly create custom ingestion helpers for anything!

## Prerequisites
- [python](https://www.python.org/)
    - [elasticsearch](https://pypi.org/project/elasticsearch/) *(`pip install elasticsearch`)*
    - [aiofiles](https://pypi.org/project/aiofiles) *(`pip install aiofiles`)*
    - [aiohttp](https://pypi.org/projects/aiohttp) *(`pip install aiohttp`)*
    - [websockets](https://pypi.org/project/websockets/) *(`pip install websockets`) (only required for `--certs` ingestion)*

## Usage
```shell
python eris.py [options] <input>
```
**Note:** The `<input>` can be a file or a directory of files, depending on the ingestion script.

### Options
###### General arguments
| Argument     | Description                                   |
|--------------|-----------------------------------------------|
| `input_path` | Path to the input file or directory           |
| `--watch`    | Create or watch a FIFO for real-time indexing |

###### Elasticsearch arguments
| Argument        | Description                                             | Default            |
|-----------------|---------------------------------------------------------|--------------------|
| `--host`        | Elasticsearch host                                      | `http://localhost` |
| `--port`        | Elasticsearch port                                      | `9200`             |
| `--user`        | Elasticsearch username                                  | `elastic`          |
| `--password`    | Elasticsearch password                                  | `$ES_PASSWORD`     |
| `--api-key`     | Elasticsearch API Key for authentication                | `$ES_APIKEY`       |
| `--self-signed` | Elasticsearch connection with a self-signed certificate |                    |

###### Elasticsearch indexing arguments
| Argument     | Description                          | Default             |
|--------------|--------------------------------------|---------------------|
| `--index`    | Elasticsearch index name             | Depends on ingestor |
| `--pipeline` | Use an ingest pipeline for the index |                     |
| `--replicas` | Number of replicas for the index     | `1`                 |
| `--shards`   | Number of shards for the index       | `1`                 |

###### Performance arguments
| Argument       | Description                                              | Default |
|----------------|----------------------------------------------------------|---------|
| `--chunk-max`  | Maximum size in MB of a chunk                            | `100`   |
| `--chunk-size` | Number of records to index in a chunk                    | `50000` |
| `--retries`    | Number of times to retry indexing a chunk before failing | `100`   |
| `--timeout`    | Number of seconds to wait before retrying a chunk        | `60`    |

###### Ingestion arguments
| Argument      | Description              |
|---------------|--------------------------|
| `--certstrem` | Index Certstream records |
| `--httpx`     | Index HTTPX records      |
| `--masscan`   | Index Masscan records    |
| `--massdns`   | Index massdns records    |
| `--zone`      | Index zone DNS records   |

~~This ingestion suite will use the built in node sniffer, so by connecting to a single node, you can load balance across the entire cluster.~~

**Note:** The sniffer has been disabled for now due an [issue](https://github.com/elastic/elasticsearch-py/issues/2005#issuecomment-1645641960) with the 8.x elasticsearch client. The auth headers are not properly sent when enabling the sniffer. A working [patch](https://github.com/elastic/elasticsearch-py/issues/2005#issuecomment-1645641960) was shared and has been *mostly* converted in [helpers/sniff_patch.py](./helpers/sniff_patch.py) for the async client.

## Roadmap
- Create a module for RIR database ingestion *(WHOIS, delegations, transfer, ASN mapping, peering, etc)*
- Dynamically update the batch metrics when the sniffer adds or removes nodes.
- Fix issue with leftover FIFO files *(catch SIGTERM / SIGINT signals)*
- Create a working patch for the async client to properly send auth headers.

___

###### Mirrors for this repository: [acid.vegas](https://git.acid.vegas/eris) • [SuperNETs](https://git.supernets.org/acidvegas/eris) • [GitHub](https://github.com/acidvegas/eris) • [GitLab](https://gitlab.com/acidvegas/eris) • [Codeberg](https://codeberg.org/acidvegas/eris)
