import json
from webmem import *


with open('webmem.json') as mem_file:
    f = mem_file.read()
    stations = json.loads(f)
st = json.dumps(stations)
#print(st)


for station in stations:
    print("---------------------------------------------")
    print(station['urlname'])
    for key, value in station.items():
        #print(f'{key}: {value}')
        pass
    print()
    parse_mem_url(station['dirname'], station['filename_prefix'], station['urlname'])
