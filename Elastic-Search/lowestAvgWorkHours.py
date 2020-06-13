from elasticsearch import Elasticsearch
from utility import store_into_csv

def lowest_average_working_hours(els_obj ,index_name, path, top):
    '''
    User lowest number of working hours
    '''
    try:
        search_query = {
            #"size": 0,
            "query" : {"match_all": {}},
            "aggs": {
                "users": {
                "terms": {
                    "field": "user_name.keyword",
                    "size": top, 
                    "order": {
                        "avg_hours": "asc"
                        }
                },
                "aggs": {
                    "avg_hours": {
                        "avg": {
                            "field": "working_minutes"
                            }
                        }
                    }
                }
            }
        }

        # get a response from the cluster
        response = els_obj.search(index=index_name, body=search_query)
        # print(response['aggregations'])
        store_into_csv(path,response)
    except Exception as err:
        print('Query output can`t save because ' +err)

def main():
    es = Elasticsearch()
    lowest_average_working_hours(els_obj=es, index_name='user_log', path='data/low_avg_working.csv',top=30)

if __name__ == "__main__":
    main()