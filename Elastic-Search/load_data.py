from elasticsearch import helpers, Elasticsearch
import csv

def load_files(els_obj, path, index_name, types, pipeline_name):
    try:
        with open(path) as files:
            reader = csv.DictReader(files)
            helpers.bulk(els_obj, reader, index=index_name, doc_type=types, pipeline=pipeline_name)
        print("Done!")
    except Exception as err:
        print('File not loaded into elsaticsearch because' + err)

def main():
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    load_files(els_obj=es, path='data/UsrLogData.csv', index_name='user_log', types='logs', pipeline_name='time-to-minutes')
    
if __name__ == "__main__":
    main()