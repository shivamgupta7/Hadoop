import csv

def store_into_csv(path, response):
    '''
    Store response into csv file.
    '''
    # after aggregation query all query output save in buckets
    # print(response['aggregations']['users']['buckets'])
    list_of_output = response['aggregations']['users']['buckets']
    # keys = list_of_output[0].keys()
    with open(path, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, ['key', 'doc_count', 'avg_hours'])
        dict_writer.writeheader()
        dict_writer.writerows(list_of_output)
