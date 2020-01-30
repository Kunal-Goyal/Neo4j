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
    str = ''
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
    def extract_values(obj):
        """Pull all values of specified key from nested JSON."""
        arr = []
        print(obj)
        def extract(obj, arr):
            """Recursively search for values of key in JSON tree."""
            if isinstance(obj, dict):
                #print("1")
                for k, v in obj.items():
                    if isinstance(v, (dict, list)):
                        arr.append(k)
                        extract(v, arr)
                    elif (k == 'name' or k == 'title'):
                        #print("111")
                        arr.append(v)
            elif isinstance(obj, list):
               # print("2")
                for item in obj:
                    #print("22")
                    extract(item, arr)
                print(arr)
                return arr

        results = extract(obj, arr)
        #print(results)
        return results       
        
    for rc in tree_json:
        keys = list(rc[0].keys())
        if(rc[0]['_type']== 'Person'):
            str = rc[0]['name']
            rv = extract_values(rc[0])
            print(rv)
        #print(rc[0])
        #print("------")
#        for key in keys:
#            print(rc[0][key])
            
    #print(str)



#print(str) 
#print(list_name)  

  
session.close()
