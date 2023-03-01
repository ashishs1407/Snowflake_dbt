import os,json

envar = os.getenv('SF_CONN')
json_obj = json.loads(envar)
# print(type(json_obj))
print(json_obj)
