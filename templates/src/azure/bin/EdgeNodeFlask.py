from flask import request
from tempfile import mkstemp
from shutil import move
from os import remove, close
import fileinput,sys
from subprocess import call

NGINX_CONF = '/etc/nginx/nginx.conf'
CONF_FILE_PATH = '/etc/nginx/conf.d/h2o.conf'
H2O_DOCS_URL = 'http://docs.h2o.ai/h2o/latest-stable/h2o-docs/azure.html#h2o-artificial-intelligence-for-hdinsight'


from flask import Flask
app = Flask(__name__)

def modify_proxy(url):
   #Create temp file
    fh, abs_path = mkstemp()
    with open(abs_path,'w') as new_file:
        with open(CONF_FILE_PATH) as old_file:
            for line in old_file:
                if "proxy_pass" in line:
                    # Modify Nginx conf file
                    new_line = "proxy_pass {0};\n".format(url)
                else:
                    new_line = line
                print(new_line)
                new_file.write(new_line)
    close(fh)
    #Remove original file
    remove(CONF_FILE_PATH)
    #Move new file
    move(abs_path, CONF_FILE_PATH)

def reload_proxy():
    call("nginx -c {} -s reload".format(NGINX_CONF), shell=True)

@app.route('/flows/<clusterid>', methods = ['POST'])
def expose_flow_ui(clusterid):
    proto = request.form['proto']
    ip = request.form['ip']
    port =  request.form['port']
    url = "{}://{}:{}".format(proto, ip, port)
    # Modify proxy configuration
    modify_proxy(url)
    # Reload proxy
    reload_proxy()
    return url

@app.route('/flows/<clusterid>', methods = ['DELETE'])
def hide_flow_ui(clusterid):
    # Modify
    modify_proxy(H2O_DOCS_URL)
    # Restart proxy
    reload_proxy()
    return


if __name__ == '__main__':
    app.run(host="0.0.0.0")

