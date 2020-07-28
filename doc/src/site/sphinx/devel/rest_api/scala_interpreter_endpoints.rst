Scala Interpreter via REST API
==============================

Basic Design
------------

A simple pool of interpreters is created at the start of each session. Once a scala interpreter is associated with the session, a new interpreter is added to the pool. Each interpreter is deleted when it is not used for some fixed time.

Example usage of Scala Interpreters using the REST API
------------------------------------------------------

Here, you can find some basic calls to access Scala interpreter behind REST API using curl.

Init interpreter and obtain a session ID
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    curl --data-urlencode '' http://192.168.0.10:54321/3/scalaint

Destroy session and interpreter associated with the session
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    curl -X DELETE
    http://192.168.0.10:54321/3/scalaint/512ef484-e21a-48f9-979e-2879f63a779e

Get all active sessions
~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    curl http://192.168.0.10:54321/3/scalaint

Interpret the incomplete code, status is error
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    curl --data-urlencode code='sc.'
    http://192.168.0.10:54321/3/scalaint/c3e5ea38-0b7e-4136-9ba3-21615ea2d298

Try to interpret the code, status is an error (function does not exist)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    curl --data-urlencode code='foo()'
    http://192.168.0.10:54321/3/scalaint/c3e5ea38-0b7e-4136-9ba3-21615ea2d298

Interpret the code, the result is success
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    curl --data-urlencode code='21+21'
    http://192.168.0.10:54321/3/scalaint/c3e5ea38-0b7e-4136-9ba3-21615ea2d298

Interpret the code with the Spark context, use the semicolon to separate commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    curl --data-urlencode code='val data = Array(1, 2, 3, 4, 5); val
    distData = sc.parallelize(data); val result = distData.map(s => s+10)'
    http://192.168.0.10:54321/3/scalaint/c3e5ea38-0b7e-4136-9ba3-21615ea2d298

Interpret the code with the Spark context, use newlines to separate commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    curl --data-urlencode code=' val data = Array(1, 2, 3, 4, 5) val
    distData = sc.parallelize(data) val result = distData.map(s => s+10)'
    http://192.168.0.10:54321/3/scalaint/c3e5ea38-0b7e-4136-9ba3-21615ea2d298

Declare class and use it in the next call
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    curl --data-urlencode code=' case class A(number: Int)'
    http://192.168.0.10:54321/3/scalaint/c3e5ea38-0b7e-4136-9ba3-21615ea2d298

    curl --data-urlencode code=' val data = Array(1, 2, 3, 4, 5) val
    distData = sc.parallelize(data) val result = distData.map(s => A(s))'
    http://192.168.0.10:54321/3/scalaint/c3e5ea38-0b7e-4136-9ba3-21615ea2d298
