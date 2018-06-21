import sys, json


print(json.load(sys.stdin)['spark'].encode('ascii','ignore')[:3])

