# coding: utf-8
import socket
import time
import random
import datetime
s = socket.socket()
s.bind(("localhost",9999))
s.listen(2)
c,addr = s.accept()
while True:
    time.sleep(0.001)
    c.send(datetime.datetime.now().isoformat() + "," + str(random.random()) + "," + str(random.random()) +"\n")
    
