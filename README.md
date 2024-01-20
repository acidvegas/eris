# Elasticsearch Recon Ingestion Scripts (ERIS)
> A utility for ingesting large scale reconnaissance data into Elast Search

### Work In Progress

## Prerequisites
- [python](https://www.python.org/)
- [elasticsearch](https://pypi.org/project/elasticsearch/) *(`pip install elasticsearch`)*

###### Options
| Argument        | Description                                                                                  |
| --------------- | -------------------------------------------------------------------------------------------- |
| `--dry-run`     | Perform a dry run without indexing records to Elasticsearch.                                 |
| `--batch_size`  | Number of records to index in a batch *(default 25,000)*.                                    |
| `--host`        | Elasticsearch host *(default 'localhost')*.                                                  |
| `--port`        | Elasticsearch port *(default 9200)*.                                                         |
| `--user`        | Elasticsearch username *(default 'elastic')*.                                                |
| `--password`    | Elasticsearch password. If not provided, it checks the environment variable **ES_PASSWORD**. |
| `--api-key`     | Elasticsearch API Key for authentication.                                                    |
| `--index`       | Elasticsearch index name *(default 'zone_files')*.                                           |
| `--filter`      | Filter out records by type *(comma-separated list)*.                                         |
| `--self-signed` | Allow self-signed certificates.                                                              |

___

###### Mirrors
[acid.vegas](https://git.acid.vegas/eris) • [GitHub](https://github.com/acidvegas/eris) • [GitLab](https://gitlab.com/acidvegas/eris) • [SuperNETs](https://git.supernets.org/acidvegas/eris)