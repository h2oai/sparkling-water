# Scala interpreter available through REST API

## Basic design
There is a simple pool of interpreters created at the start. Once an scala interpreter is associated with
the session, new interpreter is added to the pool. Each interpreter is deleted when it's not used for some
fixed time

## Example usage of scala interpreter using REST API
Here, you can find some basic calls to access scala interpreter behind REST API using curl

### init interpreter and obtain session ID
curl --data-urlencode '' http://192.168.0.10:54321/3/scalaint

### destroy session and interpreter associated with the session
curl -X DELETE http://192.168.0.10:54321/3/scalaint/512ef484-e21a-48f9-979e-2879f63a779e

### get all active sessions
curl http://192.168.0.10:54321/3/scalaint

### try to interpret the incomplete code, status is error
curl --data-urlencode code='sc.' http://192.168.0.10:54321/3/scalaint/c3e5ea38-0b7e-4136-9ba3-21615ea2d298

### try to interpret the code, status is error (function does not exist)
curl --data-urlencode code='foo()' http://192.168.0.10:54321/3/scalaint/c3e5ea38-0b7e-4136-9ba3-21615ea2d298

### try to interpret the code, result is success
curl --data-urlencode code='21+21' http://192.168.0.10:54321/3/scalaint/c3e5ea38-0b7e-4136-9ba3-21615ea2d298

### try to interpret the code with the spark context, use semicolon to separate commands
curl --data-urlencode code='val data = Array(1, 2, 3, 4, 5); val distData = sc.parallelize(data); val result = distData.map(s => s+10)' http://192.168.0.10:54321/3/scalaint/c3e5ea38-0b7e-4136-9ba3-21615ea2d298

### try to interpret the code with the spark context, use new lines to separate commands
curl --data-urlencode code='
val data = Array(1, 2, 3, 4, 5)
val distData = sc.parallelize(data)
val result = distData.map(s => s+10)' http://192.168.0.10:54321/3/scalaint/c3e5ea38-0b7e-4136-9ba3-21615ea2d298

### declare class and use it in the next call
curl --data-urlencode code='
case class A(number: Int)' http://192.168.0.10:54321/3/scalaint/c3e5ea38-0b7e-4136-9ba3-21615ea2d298

curl --data-urlencode code='
val data = Array(1, 2, 3, 4, 5)
val distData = sc.parallelize(data)
val result = distData.map(s => A(s))' http://192.168.0.10:54321/3/scalaint/c3e5ea38-0b7e-4136-9ba3-21615ea2d298

