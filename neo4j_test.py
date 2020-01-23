from neo4j import GraphDatabase
import os
import pandas as pd
import numpy as np

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "kunal"))

session = driver.session()

query = """ MATCH (root)
WHERE NOT ()-->(root) AND size((root)-->()) > 1
return root"""
list_labels = []
list_name = []
result = session.run(query)

# Pandas DataFrame
#df = pd.DataFrame(result.records, columns=result.keys)
#print(df)

#print(df.count())
for record in result:
#    print(record[0].labels)
    list_labels.append(list(record[0].labels)[0])         # convert frozenset into list
    list_name.append(record[0]['name'])

for i, el in enumerate(list_labels):                      # read enumerate
    name = list_name[i]
    query_build = ''' MATCH p = (n:'''+el+'''{name:"'''+name+'''"})-[*]->()
                      with collect(p) as path
                      CALL apoc.convert.toTree(path) yield value
                      RETURN value'''
    tree_json = session.run(query_build)
    # dir = os.path.join("C:\\", "kunal", name)
    # if not os.path.exists(dir):
        # try:
            # os.mkdir(dir)
        # except OSError as e:
            # if e.errno != errno.EEXIST:
                # raise
    
    def item_generator(json_input, lookup_key):
        if isinstance(json_input, dict):
            for k, v in json_input.items():
                if k == lookup_key:
                    yield v
                else:
                    yield from item_generator(v, lookup_key)
        elif isinstance(json_input, list):
            for item in json_input:
                yield from item_generator(item, lookup_key)           
    for rc in tree_json:
        keys = list(rc[0].keys())
 #       for _ in item_generator(rc[0],'acted_in'):
 #           print(_)
        print(rc[0])
#        for key in keys:
#            print(rc[0][key])
            
#        print('---end---')



#print(list_labels) 
#print(list_name)  

  
session.close()