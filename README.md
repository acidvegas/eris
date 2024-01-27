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

###
###### General arguments
| Argument          | Description                                                    |
|-------------------|----------------------------------------------------------------|
| `input_path`      | Path to the input file or directory                            |
| `--dry-run`       | Dry run *(do not index records to Elasticsearch)*              |
| `--watch`         | Watch the input file for new lines and index them in real time |

###### Elasticsearch arguments
| Argument          | Description                                                                          | Default       |
|-------------------|--------------------------------------------------------------------------------------|---------------|
| `--host`          | Elasticsearch host *(Will sniff for other nodes in the cluster)*                     | `localhost`   |
| `--port`          | Elasticsearch port                                                                   | `9200`        |
| `--user`          | Elasticsearch username                                                               | `elastic`     |
| `--password`      | Elasticsearch password *(if not provided, check environment variable `ES_PASSWORD`)* |               |
| `--api-key`       | Elasticsearch API Key for authentication                                             |               |
| `--self-signed`   | Elastic search instance is using a self-signed certificate                           | `true`        |
| `--index`         | Elasticsearch index name                                                             | `masscan-logs`|
| `--shards`        | Number of shards for the index                                                       | `1`           |
| `--replicas`      | Number of replicas for the index                                                     | `1`           |

###### Performance arguments
| Argument          | Description                                                                          | Default       |
|-------------------|--------------------------------------------------------------------------------------|---------------|
| `--batch-max`     | Maximum size in MB of a batch                                                        | `10`          |
| `--batch-size`    | Number of records to index in a batch                                                | `5000`        |
| `--batch-threads` | Number of threads to use when indexing in batches                                    | `2`           |
| `--retries`       | Number of times to retry indexing a batch before failing                             | `10`          |
| `--timeout`       | Number of seconds to wait before retrying a batch                                    | `30`          |

**NOTE:** Using `--batch-threads` as 4 and `--batch-size` as 10000 with 3 nodes would process 120,000 records before indexing 40,000 per node.

___

###### Mirrors for this repository: [acid.vegas](https://git.acid.vegas/eris) • [SuperNETs](https://git.supernets.org/acidvegas/eris) • [GitHub](https://github.com/acidvegas/eris) • [GitLab](https://gitlab.com/acidvegas/eris) • [Codeberg](https://codeberg.org/acidvegas/eris)