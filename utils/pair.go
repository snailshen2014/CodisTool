package utils

import (
	"sort"
)

//Pair  A data structure to hold a key/value pair.
type Pair struct {
	Key   string
	Value int
}

// PairList A slice of Pairs that implements sort.Interface to sort by Value.
type PairList []Pair

//desc sorted
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }

// SortMapByValue A function to turn a map into a PairList, then sort and return it.
func SortMapByValue(m map[string]int) PairList {
	p := make(PairList, len(m))
	i := 0
	for k, v := range m {
		// fmt.Printf("k==[%s],v==[%d]\n", k, v)
		p[i] = Pair{k, v}
		i++
	}
	sort.Sort(p)
	// fmt.Printf("sored pair:%v\n", p)
	// for _, v := range p {
	// 	fmt.Printf("sorted,key==[%s],value==[%d]\n", v.Key, v.Value)

	// }
	return p
}
