package btree

type Less func(a, b *IKey) bool

type BTree struct {
    size int64
    node bNode
    less Less
}

type kNode struct {
    size int64
    ks []IKey       // length of size
    ps []IValue     // length of size+1
}

type iNode struct {
    kNode
}

type IKey struct {
    Ctrl int32
    KeyFpos int64
    DocidPos int64
}

type IValue struct {
    ValueFpos int64
}
