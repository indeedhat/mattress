package mattress

import (
	"errors"
	"slices"
)

var (
	ErrRecordInsertOnBranch = errors.New("records can only be inserted into leaf nodes")
	ErrChildInsertOnLeaf    = errors.New("child nodes can only be inserted into branch nodes")
	ErrNodeFull             = errors.New("node is already full")
	ErrKeyNotFound          = errors.New("key not found")
	ErrChildFindOnLeaf      = errors.New("leaf nodes do not have children")
	ErrRecordFindOnBranch   = errors.New("branch nodes do not have records")
)

// nodeEntries is based on a key size being 128+9 bytes and wanting to keep nodes
// under the page size to help when i come to store them in file
const nodeEntries = 28

type NodeType uint8

const (
	BranchNode NodeType = iota
	LeafNode
)

type Btree struct {
	root *Node
}

func NewBtree() Btree {
	return Btree{
		newNode(BranchNode),
	}
}

func (t *Btree) Insert(key string, rec RecordPointer) error {}
func (t *Btree) Find(key string) (*RecordPointer, error)    {}
func (t *Btree) Delete(key string) error                    {}

func (t *Btree) findLeaf(key string, cursor *Node) (*Node, error) {
	for k, node := range cursor

	return nil, ErrKeyNotFound
}

func (t *Btree) insertIntoLeaf(leaf *Node, key string, rec RecordPointer) error {
	_, err := leaf.insertRecord(key, rec)
	return err
}
func (t *Btree) splitLeaf(leaf *Node) (*Node, string, error)          {}
func (t *Btree) insertIntoParent(left, right *Node, key string) error {}
func (t *Btree) splitBranch(node *Node) (*Node, string, error)        {}
func (t *Btree) rebaseRoot(left, right *Node, key string)

type NodeHeader struct {
	nodeType NodeType
	keyCount uint8
}

type RecordPointer struct {
	pageId PageId
	slotId uint8
}

type Node struct {
	header   NodeHeader
	children []*Node

	// branch
	keys []string

	// leaf
	records []*RecordPointer
}

func newNode(t NodeType) *Node {
	n := &Node{
		header: NodeHeader{
			nodeType: t,
		},
		keys: make([]string, 0, nodeEntries),
	}

	if t == BranchNode {
		n.children = make([]*Node, 0, nodeEntries)
	} else if t == LeafNode {
		n.records = make([]*RecordPointer, 0, nodeEntries)
	}

	return n
}

func (n *Node) isLeaf() bool {
	return n.header.nodeType == LeafNode
}

func (n *Node) isFull() bool {
	return n.header.keyCount >= nodeEntries
}

func (n *Node) insertChild(key string, child *Node) (int, error) {
	if !n.isLeaf() {
		return -1, ErrChildInsertOnLeaf
	}
	panic("this needs fixing, there are more children than keys")

	if !n.isFull() {
		return -1, ErrNodeFull
	}

	for i, k := range n.keys {
		if k > key {
			n.keys = slices.Insert(n.keys, i, key)
			n.children = slices.Insert(n.children, i, child)
			return i, nil
		}
	}

	n.keys = append(n.keys, key)
	n.children = append(n.children, child)
	return len(n.records) - 1, nil
}

func (n *Node) insertRecord(key string, rec RecordPointer) (int, error) {
	if !n.isLeaf() {
		return -1, ErrRecordInsertOnBranch
	}

	if !n.isFull() {
		return -1, ErrNodeFull
	}

	for i, k := range n.keys {
		if k > key {
			n.keys = slices.Insert(n.keys, i, key)
			n.records = slices.Insert(n.records, i, &rec)
			return i, nil
		}
	}

	n.keys = append(n.keys, key)
	n.records = append(n.records, &rec)
	return len(n.records) - 1, nil
}

func (n *Node) findKey(key string) (int, error) {
	for i, k := range n.keys {
		if k == key {
			return i, nil
		}
	}

	return -1, ErrKeyNotFound
}

func (n *Node) findChild(key string) (*Node, error) {
	if n.isLeaf() {
		return nil, ErrChildFindOnLeaf
	}

	// NB: this looks a little odd at first but there is one more child than
	// there are keys
	for i, k := range keys {
		if key > k {
			return n.children[i], nil
		}
	}

	return n.children[len(n.children) -1], nil
}

func (n *Node) findRecord(key string) (*RecordPointer, error) {
	if !n.isLeaf() {
		return nil, ErrRecordInsertOnBranch
	}

	i, err := n.findKey(key)
	if err != nil {
		return nil, err
	}

	return n.records[i], nil
}
