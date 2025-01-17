// Copyright 2012 Daniel Connelly.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package rtreego is a library for efficiently storing and querying spatial data.
package rtreego

import (
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"

	//"github.com/golang/protobuf/ptypes"
	//"github.com/golang/protobuf/ptypes/any"
	"math"
	pb "myrtreego"
	"reflect"
	"sort"
)

// Comparator compares two spatials and returns whether they are equal.
type Comparator func(obj1, obj2 Spatial) (equal bool)

func defaultComparator(obj1, obj2 Spatial) bool {
	return obj1 == obj2
}

// Rtree represents an R-tree, a balanced search tree for storing and querying
// spatial objects.  Dim specifies the number of spatial dimensions and
// MinChildren/MaxChildren specify the minimum/maximum branching factors.
type Rtree struct {
	Dim         int
	MinChildren int
	MaxChildren int
	Root        *node
	Size        int
	Height      int
}

// NewTree returns an Rtree. If the number of objects given on initialization
// is larger than max, the Rtree will be initialized using the Overlap
// Minimizing Top-down bulk-loading algorithm.
func NewTree(dim, min, max int, objs ...Spatial) *Rtree {
	rt := &Rtree{
		Dim:         dim,
		MinChildren: min,
		MaxChildren: max,
		Height:      1,
		Root: &node{
			entries: []Entry{},
			leaf:    true,
			level:   1,
		},
	}

	if len(objs) <= rt.MaxChildren {
		for _, obj := range objs {
			rt.Insert(obj)
		}
	} else {
		rt.BulkLoad(objs)
	}

	return rt
}

// Size returns the number of objects currently stored in tree.
func (tree *Rtree) MYSize() int {
	return tree.Size
}

func (tree *Rtree) String() string {
	return "foo"
}

// Depth returns the maximum depth of tree.
func (tree *Rtree) Depth() int {
	return tree.Height
}

type dimSorter struct {
	dim  int
	objs []Entry
}

func (s *dimSorter) Len() int {
	return len(s.objs)
}

func (s *dimSorter) Swap(i, j int) {
	s.objs[i], s.objs[j] = s.objs[j], s.objs[i]
}

func (s *dimSorter) Less(i, j int) bool {
	return s.objs[i].Bb.P[s.dim] < s.objs[j].Bb.P[s.dim]
}

// walkPartitions splits objs into slices of maximum k elements and
// iterates over these partitions.
func walkPartitions(k int, objs []Entry, iter func(parts []Entry)) {
	n := (len(objs) + k - 1) / k // ceil(len(objs) / k)

	for i := 1; i < n; i++ {
		iter(objs[(i-1)*k : i*k])
	}
	iter(objs[(n-1)*k:])
}

func sortByDim(dim int, objs []Entry) {
	sort.Sort(&dimSorter{dim, objs})
}

// bulkLoad bulk loads the Rtree using OMT algorithm. bulkLoad contains special
// handling for the root node.
func (tree *Rtree) BulkLoad(objs []Spatial) *pb.TreeData{
	n := len(objs)

	// create entries for all the objects
	entries := make([]Entry, n)
	for i := range objs {
		entries[i] = Entry{
			Bb:  objs[i].Bounds(),
			Obj: objs[i],
			Type:"Thing",
		}
	}

	// following equations are defined in the paper describing OMT
	var (
		N = float64(n)
		M = float64(tree.MaxChildren)
	)
	// Eq1: height of the tree
	// use log2 instead of log due to rounding errors with log,
	// eg, math.Log(9) / math.Log(3) > 2
	h := math.Ceil(math.Log2(N) / math.Log2(M))

	// Eq2: size of subtrees at the root
	nsub := math.Pow(M, h-1)

	// Inner Eq3: number of subtrees at the root
	s := math.Ceil(N / nsub)

	// Eq3: number of slices
	S := math.Floor(math.Sqrt(s))

	// sort all entries by first dimension
	sortByDim(0, entries)
	tree.Height = int(h)
	tree.Size = n
 //fmt.Println(entries)
//myp
	//newentries := make([]pb.Entry, n)
	newentries := make([]*pb.Entry, n)
	//fmt.Println("fenge.....")
//	copy(newentries, entries)
	//fmt.Println(newentries)

	for i, en := range entries {
		var an *any.Any
		an, _ = ptypes.MarshalAny(&pb.Thing{Name: en.Obj.(*Thing).Name,Location:en.Obj.(*Thing).Location})
	//	fmt.Println("an...",an)
		myeb:=pb.Entry{
			Bb:  &pb.Rect{P:en.Bb.P,Q:en.Bb.Q},
			Obj: an,
		}
		newentries[i]=&myeb
	}

	td := &pb.TreeData{
		Dim:         int32(tree.Dim),
		MinChildren: int32(tree.MinChildren),
		MaxChildren: int32(tree.MaxChildren),
		Height:      int32(tree.Height),
		Size:        int32(n),
		NSlices:     S,
		NSubtree:    s,
		MyEntries:newentries,
	}//myp end


	tree.Root = tree.Omt(int(h), int(S), entries, int(s))


	return td
}

// omt is the recursive part of the Overlap Minimizing Top-loading bulk-
// load approach. Returns the root node of a subtree.
func (tree *Rtree) Omt(level, nSlices int, objs []Entry, m int) *node {
	// if number of objects is less than or equal than max children per leaf,
	// we need to create a leaf node
	if len(objs) <= m {
		// as long as the recursion is not at the leaf, call it again
		if level > 1 {
			child := tree.Omt(level-1, nSlices, objs, m)
			n := &node{
				level: level,
				entries: []Entry{{
					Bb:    child.computeBoundingBox(),
					child: child,
				}},
			}
			child.parent = n
			return n
		}
		return &node{
			leaf:    true,
			entries: objs,
			level:   level,
		}
	}

	n := &node{
		level:   level,
		entries: make([]Entry, 0, m),
	}

	// maximum node size given at most M nodes at this level
	k := (len(objs) + m - 1) / m // = ceil(N / M)

	// In the root level, split objs in nSlices. In all other levels,
	// we use a single slice.
	vertSize := len(objs)
	if nSlices > 1 {
		vertSize = nSlices * k
	}

	// create sub trees
	walkPartitions(vertSize, objs, func(vert []Entry) {
		// sort vertical slice by a different dimension on every level
		sortByDim((tree.Height-level+1)%tree.Dim, vert)

		// split slice into groups of size k
		walkPartitions(k, vert, func(part []Entry) {
			child := tree.Omt(level-1, 1, part, tree.MaxChildren)
			child.parent = n

			n.entries = append(n.entries, Entry{
				Bb:    child.computeBoundingBox(),
				child: child,
			})
		})
	})
	return n
}
//myp data
/*
type TreeData struct {
	Dim         int
	MinChildren int
	MaxChildren int
	 Height int // height of tree
	 Size int
	 NSlices  float64//number of slices
	 NSubtree float64//number of subtrees at the root
	 Entries []Entry
}*/
// node represents a tree node of an Rtree.
type node struct {
	parent  *node
	leaf    bool
	entries []Entry
	level   int // node depth in the Rtree
}

// entry represents a spatial index record stored in a tree node.

type Entry struct {
	Bb    *Rect `json:"rect"`// bounding-box of all children of this entry
	child *node
	Obj   Spatial `json:"value"`
	Type  string `json:"type"`//specific type of Spatial
}
//myp start
func (c *Entry) UnmarshalJSON(data []byte) error {
	value,rect, err := UnmarshalCustomValue(data, "type", "value", map[string]reflect.Type{
		"Thing": reflect.TypeOf(Thing{}),
	})
	if err != nil {
		return err
	}
	c.Obj=value
	c.Bb=rect
	return nil
}
func UnmarshalCustomValue(data []byte, typeJsonField, valueJsonField string, customTypes map[string]reflect.Type) (Spatial,*Rect, error) {
	m := map[string]interface{}{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil,nil, err
	}

	typeName := m[typeJsonField].(string)
	var value Spatial
	if ty, found := customTypes[typeName]; found {
		value = reflect.New(ty).Interface().(Spatial)
	}

	valueBytes, err := json.Marshal(m[valueJsonField])
	if err != nil {
		return nil,nil, err
	}

	if err = json.Unmarshal(valueBytes, &value); err != nil {
		return nil,nil,err
	}
	myrect, err:=json.Marshal(m["rect"])
	var rectme Rect
	json.Unmarshal(myrect, &rectme)
	return value,&rectme, nil
}
//myp end
// Spatial is an interface for objects that can be stored in an Rtree and queried.
type Spatial interface {
	Bounds() *Rect
}

type Thing struct {
	Location Point
	Name string
}
var tol = 0.005

func (t *Thing) Bounds() *Rect {

	return t.Location.ToRect(tol)
}



// Insertion

// Insert inserts a spatial object into the tree.  If insertion
// causes a leaf node to overflow, the tree is rebalanced automatically.
//
// Implemented per Section 3.2 of "R-trees: A Dynamic Index Structure for
// Spatial Searching" by A. Guttman, Proceedings of ACM SIGMOD, p. 47-57, 1984.
func (tree *Rtree) Insert(obj Spatial) {
	e := Entry{obj.Bounds(), nil, obj,""}
	tree.insert(e, 1)
	tree.Size++
}

// insert adds the specified entry to the tree at the specified level.
func (tree *Rtree) insert(e Entry, level int) {
	leaf := tree.chooseNode(tree.Root, e, level)
	leaf.entries = append(leaf.entries, e)

	// update parent pointer if necessary
	if e.child != nil {
		e.child.parent = leaf
	}

	// split leaf if overflows
	var split *node
	if len(leaf.entries) > tree.MaxChildren {
		leaf, split = leaf.split(tree.MinChildren)
	}
	root, splitRoot := tree.adjustTree(leaf, split)
	if splitRoot != nil {
		oldRoot := root
		tree.Height++
		tree.Root = &node{
			parent: nil,
			level:  tree.Height,
			entries: []Entry{
				Entry{Bb: oldRoot.computeBoundingBox(), child: oldRoot},
				Entry{Bb: splitRoot.computeBoundingBox(), child: splitRoot},
			},
		}
		oldRoot.parent = tree.Root
		splitRoot.parent = tree.Root
	}
}

// chooseNode finds the node at the specified level to which e should be added.
func (tree *Rtree) chooseNode(n *node, e Entry, level int) *node {
	if n.leaf || n.level == level {
		return n
	}

	// find the entry whose bb needs least enlargement to include obj
	diff := math.MaxFloat64
	var chosen Entry
	for _, en := range n.entries {
		bb := boundingBox(en.Bb, e.Bb)
		d := bb.size() - en.Bb.size()
		if d < diff || (d == diff && en.Bb.size() < chosen.Bb.size()) {
			diff = d
			chosen = en
		}
	}

	return tree.chooseNode(chosen.child, e, level)
}

// adjustTree splits overflowing nodes and propagates the changes upwards.
func (tree *Rtree) adjustTree(n, nn *node) (*node, *node) {
	// Let the caller handle root adjustments.
	if n == tree.Root {
		return n, nn
	}

	// Re-size the bounding box of n to account for lower-level changes.
	en := n.getEntry()
	en.Bb = n.computeBoundingBox()

	// If nn is nil, then we're just propagating changes upwards.
	if nn == nil {
		return tree.adjustTree(n.parent, nil)
	}

	// Otherwise, these are two nodes resulting from a split.
	// n was reused as the "left" node, but we need to add nn to n.parent.
	enn := Entry{nn.computeBoundingBox(), nn, nil,""}
	n.parent.entries = append(n.parent.entries, enn)

	// If the new entry overflows the parent, split the parent and propagate.
	if len(n.parent.entries) > tree.MaxChildren {
		return tree.adjustTree(n.parent.split(tree.MinChildren))
	}

	// Otherwise keep propagating changes upwards.
	return tree.adjustTree(n.parent, nil)
}

// getEntry returns a pointer to the entry for the node n from n's parent.
func (n *node) getEntry() *Entry {
	var e *Entry
	for i := range n.parent.entries {
		if n.parent.entries[i].child == n {
			e = &n.parent.entries[i]
			break
		}
	}
	return e
}

// computeBoundingBox finds the MBR of the children of n.
func (n *node) computeBoundingBox() (bb *Rect) {
	childBoxes := make([]*Rect, len(n.entries))
	for i, e := range n.entries {
		childBoxes[i] = e.Bb
	}
	bb = boundingBoxN(childBoxes...)
	return
}

// split splits a node into two groups while attempting to minimize the
// bounding-box area of the resulting groups.
func (n *node) split(minGroupSize int) (left, right *node) {
	// find the initial split
	l, r := n.pickSeeds()
	leftSeed, rightSeed := n.entries[l], n.entries[r]

	// get the entries to be divided between left and right
	remaining := append(n.entries[:l], n.entries[l+1:r]...)
	remaining = append(remaining, n.entries[r+1:]...)

	// setup the new split nodes, but re-use n as the left node
	left = n
	left.entries = []Entry{leftSeed}
	right = &node{
		parent:  n.parent,
		leaf:    n.leaf,
		level:   n.level,
		entries: []Entry{rightSeed},
	}

	// TODO
	if rightSeed.child != nil {
		rightSeed.child.parent = right
	}
	if leftSeed.child != nil {
		leftSeed.child.parent = left
	}

	// distribute all of n's old entries into left and right.
	for len(remaining) > 0 {
		next := pickNext(left, right, remaining)
		e := remaining[next]

		if len(remaining)+len(left.entries) <= minGroupSize {
			assign(e, left)
		} else if len(remaining)+len(right.entries) <= minGroupSize {
			assign(e, right)
		} else {
			assignGroup(e, left, right)
		}

		remaining = append(remaining[:next], remaining[next+1:]...)
	}

	return
}

// getAllBoundingBoxes traverses tree populating slice of bounding boxes of non-leaf nodes.
func (n *node) getAllBoundingBoxes() []*Rect {
	var rects []*Rect
	if n.leaf {
		return rects
	}
	for _, e := range n.entries {
		if e.child == nil {
			return rects
		}
		rectsInter := append(e.child.getAllBoundingBoxes(), e.Bb)
		rects = append(rects, rectsInter...)
	}
	return rects
}

func assign(e Entry, group *node) {
	if e.child != nil {
		e.child.parent = group
	}
	group.entries = append(group.entries, e)
}

// assignGroup chooses one of two groups to which a node should be added.
func assignGroup(e Entry, left, right *node) {
	leftBB := left.computeBoundingBox()
	rightBB := right.computeBoundingBox()
	leftEnlarged := boundingBox(leftBB, e.Bb)
	rightEnlarged := boundingBox(rightBB, e.Bb)

	// first, choose the group that needs the least enlargement
	leftDiff := leftEnlarged.size() - leftBB.size()
	rightDiff := rightEnlarged.size() - rightBB.size()
	if diff := leftDiff - rightDiff; diff < 0 {
		assign(e, left)
		return
	} else if diff > 0 {
		assign(e, right)
		return
	}

	// next, choose the group that has smaller area
	if diff := leftBB.size() - rightBB.size(); diff < 0 {
		assign(e, left)
		return
	} else if diff > 0 {
		assign(e, right)
		return
	}

	// next, choose the group with fewer entries
	if diff := len(left.entries) - len(right.entries); diff <= 0 {
		assign(e, left)
		return
	}
	assign(e, right)
}

// pickSeeds chooses two child entries of n to start a split.
func (n *node) pickSeeds() (int, int) {
	left, right := 0, 1
	maxWastedSpace := -1.0
	for i, e1 := range n.entries {
		for j, e2 := range n.entries[i+1:] {
			d := boundingBox(e1.Bb, e2.Bb).size() - e1.Bb.size() - e2.Bb.size()
			if d > maxWastedSpace {
				maxWastedSpace = d
				left, right = i, j+i+1
			}
		}
	}
	return left, right
}

// pickNext chooses an entry to be added to an entry group.
func pickNext(left, right *node, entries []Entry) (next int) {
	maxDiff := -1.0
	leftBB := left.computeBoundingBox()
	rightBB := right.computeBoundingBox()
	for i, e := range entries {
		d1 := boundingBox(leftBB, e.Bb).size() - leftBB.size()
		d2 := boundingBox(rightBB, e.Bb).size() - rightBB.size()
		d := math.Abs(d1 - d2)
		if d > maxDiff {
			maxDiff = d
			next = i
		}
	}
	return
}

// Deletion

// Delete removes an object from the tree.  If the object is not found, returns
// false, otherwise returns true. Uses the default comparator when checking
// equality.
//
// Implemented per Section 3.3 of "R-trees: A Dynamic Index Structure for
// Spatial Searching" by A. Guttman, Proceedings of ACM SIGMOD, p. 47-57, 1984.
func (tree *Rtree) Delete(obj Spatial) bool {
	return tree.DeleteWithComparator(obj, defaultComparator)
}

// DeleteWithComparator removes an object from the tree using a custom
// comparator for evaluating equalness. This is useful when you want to remove
// an object from a tree but don't have a pointer to the original object
// anymore.
func (tree *Rtree) DeleteWithComparator(obj Spatial, cmp Comparator) bool {
	n := tree.findLeaf(tree.Root, obj, cmp)
	if n == nil {
		return false
	}

	ind := -1
	for i, e := range n.entries {
		if cmp(e.Obj, obj) {
			ind = i
		}
	}
	if ind < 0 {
		return false
	}

	n.entries = append(n.entries[:ind], n.entries[ind+1:]...)

	tree.condenseTree(n)
	tree.Size--

	if !tree.Root.leaf && len(tree.Root.entries) == 1 {
		tree.Root = tree.Root.entries[0].child
	}

	tree.Height = tree.Root.level

	return true
}

// findLeaf finds the leaf node containing obj.
func (tree *Rtree) findLeaf(n *node, obj Spatial, cmp Comparator) *node {
	if n.leaf {
		return n
	}
	// if not leaf, search all candidate subtrees
	for _, e := range n.entries {
		if e.Bb.containsRect(obj.Bounds()) {
			leaf := tree.findLeaf(e.child, obj, cmp)
			if leaf == nil {
				continue
			}
			// check if the leaf actually contains the object
			for _, leafEntry := range leaf.entries {
				if cmp(leafEntry.Obj, obj) {
					return leaf
				}
			}
		}
	}
	return nil
}

// condenseTree deletes underflowing nodes and propagates the changes upwards.
func (tree *Rtree) condenseTree(n *node) {
	deleted := []*node{}

	for n != tree.Root {
		if len(n.entries) < tree.MinChildren {
			// remove n from parent entries
			entries := []Entry{}
			for _, e := range n.parent.entries {
				if e.child != n {
					entries = append(entries, e)
				}
			}
			if len(n.parent.entries) == len(entries) {
				panic(fmt.Errorf("Failed to remove entry from parent"))
			}
			n.parent.entries = entries

			// only add n to deleted if it still has children
			if len(n.entries) > 0 {
				deleted = append(deleted, n)
			}
		} else {
			// just a child entry deletion, no underflow
			n.getEntry().Bb = n.computeBoundingBox()
		}
		n = n.parent
	}

	for _, n := range deleted {
		// reinsert entry so that it will remain at the same level as before
		e := Entry{n.computeBoundingBox(), n, nil,""}
		tree.insert(e, n.level+1)
	}
}

// Searching

// SearchIntersect returns all objects that intersect the specified rectangle.
// Implemented per Section 3.1 of "R-trees: A Dynamic Index Structure for
// Spatial Searching" by A. Guttman, Proceedings of ACM SIGMOD, p. 47-57, 1984.
func (tree *Rtree) SearchIntersect(bb *Rect, filters ...Filter) []Spatial {
	return tree.searchIntersect([]Spatial{}, tree.Root, bb, filters)
}

// SearchIntersectWithLimit is similar to SearchIntersect, but returns
// immediately when the first k results are found. A negative k behaves exactly
// like SearchIntersect and returns all the results.
//
// Kept for backwards compatibility, please use SearchIntersect with a
// LimitFilter.
func (tree *Rtree) SearchIntersectWithLimit(k int, bb *Rect) []Spatial {
	// backwards compatibility, previous implementation didn't limit results if
	// k was negative.
	if k < 0 {
		return tree.SearchIntersect(bb)
	}
	return tree.SearchIntersect(bb, LimitFilter(k))
}

func (tree *Rtree) searchIntersect(results []Spatial, n *node, bb *Rect, filters []Filter) []Spatial {
	for _, e := range n.entries {
		if intersect(e.Bb, bb) == nil {
			continue
		}

		if !n.leaf {
			results = tree.searchIntersect(results, e.child, bb, filters)
			continue
		}

		refuse, abort := applyFilters(results, e.Obj, filters)
		if !refuse {
			results = append(results, e.Obj)
		}

		if abort {
			break
		}
	}
	return results
}

// NearestNeighbor returns the closest object to the specified point.
// Implemented per "Nearest Neighbor Queries" by Roussopoulos et al
func (tree *Rtree) NearestNeighbor(p Point) Spatial {
	obj, _ := tree.nearestNeighbor(p, tree.Root, math.MaxFloat64, nil)
	return obj
}

// GetAllBoundingBoxes returning slice of bounding boxes by traversing tree. Slice
// includes bounding boxes from all non-leaf nodes.
func (tree *Rtree) GetAllBoundingBoxes() []*Rect {
	var rects []*Rect
	if tree.Root != nil {
		rects = tree.Root.getAllBoundingBoxes()
	}
	return rects
}
//myp start
/*
type node struct {
	parent  *node
	leaf    bool
	entries []entry
	level   int // node depth in the Rtree
}

func (n *node) String() string {
	return fmt.Sprintf("node{leaf: %v, entries: %v}", n.leaf, n.entries)
}

// entry represents a spatial index record stored in a tree node.
type entry struct {
	bb    *Rect // bounding-box of all children of this entry
	child *node
	obj   Spatial
}

func (e entry) String() string {
	if e.child != nil {
		return fmt.Sprintf("entry{bb: %v, child: %v}", e.bb, e.child)
	}
	return fmt.Sprintf("entry{bb: %v, obj: %v}", e.bb, e.obj)
}
 */
/*
func (n *node) String() string {
return fmt.Sprintf("node{parent:%v,leaf: %v, entries: %v,level:%v}\n",n.parent, n.leaf, n.entries,n.level)
	//return fmt.Sprintf("node{parent:%v,leaf: %v, level:%v}\n",n.parent, n.leaf, n.level)
}
func (e entry) String() string {
	if e.child != nil {
		return fmt.Sprintf("entry{bb: %v, child: %v}", e.bb, e.child)
	}
	return fmt.Sprintf("entry{bb: %v, obj: %v}", e.bb, e.obj)
}*/
func (n *node) String() string {
	return fmt.Sprintf("node{leaf: %v, level:%v, entries: %v}\n", n.leaf, n.level,n.entries)
}
func (e Entry) String() string {
	if e.child != nil {
		return fmt.Sprintf("entry{bb: %v, child: %v}\n", e.Bb, e.child)
	}
	return fmt.Sprintf("entry{bb: %v, obj: %v}\n", e.Bb, e.Obj)
}
func (tree *Rtree) Traverse() []string {
	var serialstr []string
	if tree.Root != nil {
		serialstr = tree.Root.getSerialstr()
	}
	return serialstr
}
func (n *node) getSerialstr() []string {
	var serialstr []string
	serialstr = append(serialstr, n.String())
/*
	for _, e := range n.entries {
		if e.child == nil {
			return serialstr
		}
		rectsInter := e.child.getSerialstr()
		serialstr = append(serialstr, rectsInter...)
	}*/
	return serialstr
}
// myp end


// utilities for sorting slices of entries

type entrySlice struct {
	entries []Entry
	dists   []float64
}

func (s entrySlice) Len() int { return len(s.entries) }

func (s entrySlice) Swap(i, j int) {
	s.entries[i], s.entries[j] = s.entries[j], s.entries[i]
	s.dists[i], s.dists[j] = s.dists[j], s.dists[i]
}

func (s entrySlice) Less(i, j int) bool {
	return s.dists[i] < s.dists[j]
}

func sortEntries(p Point, entries []Entry) ([]Entry, []float64) {
	sorted := make([]Entry, len(entries))
	dists := make([]float64, len(entries))
	return sortPreallocEntries(p, entries, sorted, dists)
}

func sortPreallocEntries(p Point, entries, sorted []Entry, dists []float64) ([]Entry, []float64) {
	// use preallocated slices
	sorted = sorted[:len(entries)]
	dists = dists[:len(entries)]

	for i := 0; i < len(entries); i++ {
		sorted[i] = entries[i]
		dists[i] = p.minDist(entries[i].Bb)
	}
	sort.Sort(entrySlice{sorted, dists})
	return sorted, dists
}

func pruneEntries(p Point, entries []Entry, minDists []float64) []Entry {
	minMinMaxDist := math.MaxFloat64
	for i := range entries {
		minMaxDist := p.minMaxDist(entries[i].Bb)
		if minMaxDist < minMinMaxDist {
			minMinMaxDist = minMaxDist
		}
	}
	// remove all entries with minDist > minMinMaxDist
	pruned := []Entry{}
	for i := range entries {
		if minDists[i] <= minMinMaxDist {
			pruned = append(pruned, entries[i])
		}
	}
	return pruned
}

func pruneEntriesMinDist(d float64, entries []Entry, minDists []float64) []Entry {
	var i int
	for ; i < len(entries); i++ {
		if minDists[i] > d {
			break
		}
	}
	return entries[:i]
}

func (tree *Rtree) nearestNeighbor(p Point, n *node, d float64, nearest Spatial) (Spatial, float64) {
	if n.leaf {
		for _, e := range n.entries {
			dist := math.Sqrt(p.minDist(e.Bb))
			if dist < d {
				d = dist
				nearest = e.Obj
			}
		}
	} else {
		branches, dists := sortEntries(p, n.entries)
		branches = pruneEntries(p, branches, dists)
		for _, e := range branches {
			subNearest, dist := tree.nearestNeighbor(p, e.child, d, nearest)
			if dist < d {
				d = dist
				nearest = subNearest
			}
		}
	}

	return nearest, d
}

// NearestNeighbors gets the closest Spatials to the Point.
func (tree *Rtree) NearestNeighbors(k int, p Point, filters ...Filter) ([]Spatial,[]float64){
	// preallocate the buffers for sortings the branches. At each level of the
	// tree, we slide the buffer by the number of entries in the node.
	maxBufSize := tree.MaxChildren * tree.Depth()
	branches := make([]Entry, maxBufSize)
	branchDists := make([]float64, maxBufSize)

	// allocate the buffers for the results
	dists := make([]float64, 0, k)
	objs := make([]Spatial, 0, k)

	objs, dists, _ = tree.nearestNeighbors(k, p, tree.Root, dists, objs, filters, branches, branchDists)
	return objs,dists
}

// insert obj into nearest and return the first k elements in increasing order.
func insertNearest(k int, dists []float64, nearest []Spatial, dist float64, obj Spatial, filters []Filter) ([]float64, []Spatial, bool) {
	i := sort.SearchFloat64s(dists, dist)
	for i < len(nearest) && dist >= dists[i] {
		i++
	}
	if i >= k {
		return dists, nearest, false
	}

	if refuse, abort := applyFilters(nearest, obj, filters); refuse || abort {
		return dists, nearest, abort
	}

	// no resize since cap = k
	if len(nearest) < k {
		dists = append(dists, 0)
		nearest = append(nearest, nil)
	}

	left, right := dists[:i], dists[i:len(dists)-1]
	copy(dists, left)
	copy(dists[i+1:], right)
	dists[i] = dist

	leftObjs, rightObjs := nearest[:i], nearest[i:len(nearest)-1]
	copy(nearest, leftObjs)
	copy(nearest[i+1:], rightObjs)
	nearest[i] = obj

	return dists, nearest, false
}

func (tree *Rtree) nearestNeighbors(k int, p Point, n *node, dists []float64, nearest []Spatial, filters []Filter, b []Entry, bd []float64) ([]Spatial, []float64, bool) {
	var abort bool
	if n.leaf {
		for _, e := range n.entries {
			dist := p.minDist(e.Bb)
			dists, nearest, abort = insertNearest(k, dists, nearest, dist, e.Obj, filters)
			if abort {
				break
			}
		}
	} else {
		branches, branchDists := sortPreallocEntries(p, n.entries, b, bd)
		// only prune if buffer has k elements
		if l := len(dists); l >= k {
			branches = pruneEntriesMinDist(dists[l-1], branches, branchDists)
		}
		for _, e := range branches {
			nearest, dists, abort = tree.nearestNeighbors(k, p, e.child, dists, nearest, filters, b[len(n.entries):], bd[len(n.entries):])
			if abort {
				break
			}
		}
	}
	return nearest, dists, abort
}
