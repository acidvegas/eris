# Elasticsearch Recon Ingestion Scripts (ERIS)
> A utility for ingesting various large scale reconnaissance data logs into Elasticsearch

### Work In Progress

## Prerequisites
- [python](https://www.python.org/)
    - [elasticsearch](https://pypi.org/project/elasticsearch/) *(`pip install elasticsearch`)*

## Usage
```shell
python ingest_XXXX.py [options] <input>
```
**Note:** The `<input>` can be a file or a directory of files, depending on the ingestion script.

## Operations
This ingestion suite will use the built in node sniffer, so by connecting to a single node, you can load balance across the entire cluster.
It is good to know how much nodes you have in the cluster to determine how to fine tune the arguments for the best performance, based on your environment.

###
###### General arguments
| Argument          | Description                                                    |
|-------------------|----------------------------------------------------------------|
| `input_path`      | Path to the input file or directory                            |
| `--dry-run`       | Dry run *(do not index records to Elasticsearch)*              |
| `--watch`         | Watch the input file for new lines and index them in real time |

###### Elasticsearch arguments
| Argument          | Description                                            | Default        |
|-------------------|--------------------------------------------------------|----------------|
| `--host`          | Elasticsearch host                                     | `localhost`    |
| `--port`          | Elasticsearch port                                     | `9200`         |
| `--user`          | Elasticsearch username                                 | `elastic`      |
| `--password`      | Elasticsearch password                                 | `$ES_PASSWORD` |
| `--api-key`       | Elasticsearch API Key for authentication               |                |
| `--self-signed`   | Elasticsearch connection with aself-signed certificate |                |

###### Elasticsearch indexing arguments
| Argument          | Description                      | Default             |
|-------------------|----------------------------------|---------------------|
| `--index`         | Elasticsearch index name         | Depends on ingestor |
| `--shards`        | Number of shards for the index   | `1`                 |
| `--replicas`      | Number of replicas for the index | `1`                 |

###### Performance arguments
| Argument          | Description                                              | Default |
|-------------------|----------------------------------------------------------|---------|
| `--batch-max`     | Maximum size in MB of a batch                            | `10`    |
| `--batch-size`    | Number of records to index in a batch                    | `5000`  |
| `--batch-threads` | Number of threads to use when indexing in batches        | `2`     |
| `--retries`       | Number of times to retry indexing a batch before failing | `10`    |
| `--timeout`       | Number of seconds to wait before retrying a batch        | `30`    |

Using `--batch-threads` as 4 and `--batch-size` as 10000 with 3 nodes would process 120,000 records before indexing 40,000 per node. Take these kind of metrics int account when consider how much records you want to process at once and the memory limitations of your environment, aswell as the networking constraint it may have ono your node(s), depending on the size of your cluster.

___

###### Mirrors for this repository: [acid.vegas](https://git.acid.vegas/eris) • [SuperNETs](https://git.supernets.org/acidvegas/eris) • [GitHub](https://github.com/acidvegas/eris) • [GitLab](https://gitlab.com/acidvegas/eris) • [Codeberg](https://codeberg.org/acidvegas/eris)