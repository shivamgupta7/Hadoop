from elasticsearch.client.ingest import IngestClient
from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
pipeline = IngestClient(es)
pipeline.put_pipeline(id='time-to-minutes', body={
    'description': "Extract time for datetime and convert into minutes",
    'processors': [
    {
      "dissect": {
        "field": "working_hours",
        "pattern": "%{?date} %{tmp_hours}:%{tmp_minutes}:%{?seconds}"
      }
    },
    {
      "convert": {
        "field": "tmp_hours",
        "type": "integer"
      }
    },
    {
      "convert": {
        "field": "tmp_minutes",
        "type": "integer"
      }
    },
    {
      "script": {
        "source": "\n ctx.working_minutes = (ctx.tmp_hours * 60) + ctx.tmp_minutes;\n "
      }
    },
    {
      "remove": {
        "field": [
          "tmp_hours",
          "tmp_minutes"
        ]
      }
    },
    {
      "dissect": {
        "field": "idle_time",
        "pattern": "%{?date} %{tmp_hours}:%{tmp_minutes}:%{?seconds}"
      }
    },
    {
      "convert": {
        "field": "tmp_hours",
        "type": "integer"
      }
    },
    {
      "convert": {
        "field": "tmp_minutes",
        "type": "integer"
      }
    },
    {
      "script": {
        "source": "\n ctx.idle_minutes = (ctx.tmp_hours * 60) + ctx.tmp_minutes;\n "
      }
    },
    {
      "remove": {
        "field": [
          "date",
          "tmp_hours",
          "tmp_minutes"
        ]
      }
    }
    ]
})