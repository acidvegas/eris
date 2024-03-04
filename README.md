# Elasticsearch Recon Ingestion Scripts (ERIS)
> A utility for ingesting various large scale reconnaissance data logs into Elasticsearch

### Work In Progress

## Prerequisites
- [python](https://www.python.org/)
    - [elasticsearch](https://pypi.org/project/elasticsearch/) *(`pip install elasticsearch`)*

## Usage
```shell
python eris.py [options] <input>
```
**Note:** The `<input>` can be a file or a directory of files, depending on the ingestion script.

### Options
###### General arguments
| Argument          | Description                                                    |
|-------------------|----------------------------------------------------------------|
| `input_path`      | Path to the input file or directory                            |
| `--dry-run`       | Dry run *(do not index records to Elasticsearch)*              |
| `--watch`         | Create or watch a FIFO for real-time indexing                  |

###### Elasticsearch arguments
| Argument          | Description                                                                    | Default        |
|-------------------|--------------------------------------------------------------------------------|----------------|
| `--host`          | Elasticsearch host                                                             | `localhost`    |
| `--port`          | Elasticsearch port                                                             | `9200`         |
| `--user`          | Elasticsearch username                                                         | `elastic`      |
| `--password`      | Elasticsearch password                                                         | `$ES_PASSWORD` |
| `--api-key`       | Elasticsearch API Key for authentication *(format must be api_key:api_secret)* | `$ES_APIKEY`   |
| `--self-signed`   | Elasticsearch connection with a self-signed certificate                        |                |

###### Elasticsearch indexing arguments
| Argument          | Description                          | Default             |
|-------------------|--------------------------------------|---------------------|
| `--index`         | Elasticsearch index name             | Depends on ingestor |
| `--pipeline`      | Use an ingest pipeline for the index |                     |
| `--replicas`      | Number of replicas for the index     | `1`                 |
| `--shards`        | Number of shards for the index       | `1`                 |

###### Performance arguments
| Argument          | Description                                              | Default |
|-------------------|----------------------------------------------------------|---------|
| `--chunk-max`     | Maximum size in MB of a chunk                            | `10`    |
| `--chunk-size`    | Number of records to index in a chunk                    | `5000`  |
| `--chunk-threads` | Number of threads to use when indexing in chunks         | `2`     |
| `--retries`       | Number of times to retry indexing a chunk before failing | `10`    |
| `--timeout`       | Number of seconds to wait before retrying a chunk        | `30`    |

###### Ingestion arguments
| Argument          | Description            |
|-------------------|------------------------|
| `--httpx`         | Index HTTPX records    |
| `--masscan`       | Index Masscan records  |
| `--massdns`       | Index massdns records  |
| `--zone`          | Index zone DNS records |

Using `--batch-threads` as 4 and `--batch-size` as 10000 with 3 nodes would process 120,000 records before indexing 40,000 per node. Take these kind of metrics into account when consider how much records you want to process at once and the memory limitations of your environment, aswell as the networking constraint it may have ono your node(s), depending on the size of your cluster.

This ingestion suite will use the built in node sniffer, so by connecting to a single node, you can load balance across the entire cluster.
It is good to know how much nodes you have in the cluster to determine how to fine tune the arguments for the best performance, based on your environment.

## GeoIP Pipeline
Create & add a geoip pipeline and use the following in your index mappings:

```json
"geoip": {
    "city_name": "City",
    "continent_name": "Continent",
    "country_iso_code": "CC",
    "country_name": "Country",
    "location": {
        "lat": 0.0000,
        "lon": 0.0000
    },
    "region_iso_code": "RR",
    "region_name": "Region"
}
```

## Changelog
- The `--watch` feature now uses a FIFO to do live ingestion.
- Isolated eris.py into it's own file and seperated the ingestion agents into their own modules.

## Roadmap
- Implement [async elasticsearch](https://elasticsearch-py.readthedocs.io/en/v8.12.1/async.html) into the code.

___

###### Mirrors for this repository: [acid.vegas](https://git.acid.vegas/eris) • [SuperNETs](https://git.supernets.org/acidvegas/eris) • [GitHub](https://github.com/acidvegas/eris) • [GitLab](https://gitlab.com/acidvegas/eris) • [Codeberg](https://codeberg.org/acidvegas/eris)