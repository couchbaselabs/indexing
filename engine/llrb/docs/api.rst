New()
    Return new instance of the tree.

SetRoot()
    sets the root node of the tree.
    It is intended to be used by functions that deserialize the tree

Root()
    returns the root node of the tree.
    It is intended to be used by functions that serialize the tree.

Len()
    returns the number of nodes in the tree.

Has()
    returns true if the tree contains an element whose order is the same
    as that of key.

Get()
    retrieves an element from the tree whose order is the same as that of key

Min()
    returns the minimum element in the tree.

Max()
    returns the maximum element in the tree.

InsertMany()
    insert more than one keys.

AddMany()
    add more than one keys.

Insert()
    inserts key into the tree. If an existing element has the same
    order, it is removed from the tree and returned.

Add()
    inserts key into the tree. If an existing element has the same
    order, both elements remain in the tree.

DeleteMin()
    delete key with minimum value in the tree and returns the deleted
    key or nil otherwise.

DeleteMax()
    delete key with maximum value in the tree and returns the deleted
    key or nil otherwise

Delete()
    delete key from the tree whose key equals key. The deleted key is return,
    otherwise nil is returned.

func (t &LLRB) AscendGreaterOrEqual(pivot api.Key, iterator KeyIterator)
    will call iterator once for each element greater or equal to pivot in
    ascending order. It will stop whenever the iterator returns false.

func (t &LLRB) AscendLessThan(pivot api.Key, iterator KeyIterator)
    will call iterator once for each element less thatn pivot in ascending order.
    It will stop whenever the iterator returns false.

func (t &LLRB) DescendLessOrEqual(pivot api.Key, iterator KeyIterator)
    will call iterator once for each element less than the pivot in descending
    order. It will stop whenever the iterator returns false.

func (t &LLRB) AscendRange(greaterOrEqual, lessThan api.Key, iterator KeyIterator)
    start from greaterOrEqual and till lessThan.

