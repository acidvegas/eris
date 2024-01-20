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

###### Options
| Argument        | Description                                                                                  |
| --------------- | -------------------------------------------------------------------------------------------- |
| `--dry-run`     | Perform a dry run without indexing records to Elasticsearch.                                 |
| `--batch_size`  | Number of records to index in a batch *(default 25,000)*.                                    |

###### Elasticsearch Connnection Options
| Argument        | Description                                                                                  |
| --------------- | -------------------------------------------------------------------------------------------- |
| `--host`        | Elasticsearch host *(default 'localhost')*.                                                  |
| `--port`        | Elasticsearch port *(default 9200)*.                                                         |
| `--user`        | Elasticsearch username *(default 'elastic')*.                                                |
| `--password`    | Elasticsearch password. If not provided, it checks the environment variable **ES_PASSWORD**. |
| `--api-key`     | Elasticsearch API Key for authentication.                                                    |
| `--self-signed` | Allow self-signed certificates.                                                              |

###### Elasticsearch Index Options
| Argument        | Description                                                                                  |
| --------------- | -------------------------------------------------------------------------------------------- |
| `--index`       | Elasticsearch index name *(default 'zone_files')*.                                           |
| `--replicas`    | Number of replicas for the index.                                                            |
| `--shards`      | Number of shards for the index                                                               |

___

###### Mirrors
[acid.vegas](https://git.acid.vegas/eris) • [GitHub](https://github.com/acidvegas/eris) • [GitLab](https://gitlab.com/acidvegas/eris) • [SuperNETs](https://git.supernets.org/acidvegas/eris)