import sys, json

for host in json.load(sys.stdin)['items']:	
    dns = host["Hosts"]["host_name"].split('-')[0].encode('ascii','ignore')
    if 'e' in dns and 'ed0' not in dns:
        print host["Hosts"]["host_name"]
        break