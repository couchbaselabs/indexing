To install protobuf for indexing:
---------------------------------

You must first have Go (version 1.1) installed
(see http://golang.org/doc/install).

Next, install the standard protocol buffer implementation from
http://code.google.com/p/protobuf/; you must be running version 2.5.0 or higher

.. code-block:: bash
    # For mac
    brew install protobuf

Get protobuf for go,

.. code-block:: bash
    go get code.google.com/p/goprotobuf/{proto,protoc-gen-go}
    go get -u code.google.com/p/goprotobuf/...

Compile protobuf for secondary index,

.. code-block:: bash
    cd indexing/cap
    make

Benchmark,

.. code-block:: bash
    go test -test.bench=.
