- 1 Block contains N Nodes, typically N is 1.
- first block contains,

  - 64 bit file-position for root-node.
  - list of 64 bit file-positions pointing to free nodes.
  - if file-position says 0, then it is to be discarded.

- btree node
  - 64 bit count of key-entries. we will refer this as C.
  - array of key entries, array size must be equal to C.
  - array of value entries, array size must be equal to C+1.

- key entry
  - 32 bit control field.
  - 64 bit file position pointing to location of ikey.
  - 64 bit file position pointing to location of doc-id.

- value entry
  - 64 bit file positions.
  - for leaf nodes,
    - 64 bit file position pointing to location of ivalue.
    - last entry will be 64 bit file-position pointing to location of next 
      leaf node in sort order.
  - for intermediate nodes,
    - 64 bit file position pointing to child node.
    - last entry will be 64 bit file-position pointing to location of child
      node.


Calculating node size

size of each key entry = 20 bytes   }
size of each value entry = 8 bytes  } = 28 bytes

If node size = 8192, we can store 292 entries
    

- if size of block in M bytes, each node contains M / entries of k,v pairs and 8
  bytes for next-sibling
