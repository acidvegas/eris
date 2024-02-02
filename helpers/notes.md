# Create a GeoIP ingestion pipeline

My notes for creating an ingestion pipeline for geoip usage in Kibanas maps

###### Create the ingestion pipeline
```
PUT _ingest/pipeline/geoip
{
  "description" : "Add geoip info",
  "processors" : [
    {
      "geoip" : {
        "field" : "ip",
        "ignore_missing": true
      }
    }
  ]
}
```

###### Update an index
```
PUT my_ip_locations
{
  "mappings": {
    "properties": {
      "geoip": {
        "properties": {
          "location": { "type": "geo_point" }
        }
      }
    }
  }
}
```

or...

###### Create the index
```
PUT /masscan-data
{
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 1
  },
 "mappings": {
    "properties": {
      "banner": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "geoip": {
        "properties": {
          "city_name": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "continent_name": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "country_iso_code": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "country_name": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "location": {
            "type": "geo_point"
          },
          "region_iso_code": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "region_name": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          }
        }
      },
      "ip": {
        "type": "ip"
      },
      "port": {
        "type": "integer"
      },
      "proto": {
        "type": "keyword"
      },
      "ref_id": {
        "type": "keyword"
      },
      "seen": {
        "type": "date"
      },
      "service": {
        "type": "keyword"
      }
    }
  }
}
```
