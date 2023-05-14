package query

import (
	"fmt"
	"sync"
)

// Oxla encodes a Oxla request. This will be serialized for use
// by the tsbs_run_queries_oxla program.
type Oxla struct {
	HumanLabel       []byte
	HumanDescription []byte

	Table    []byte // e.g. "cpu"
	SqlQuery []byte
	id       uint64
}

// OxlaPool is a sync.Pool of OxlaDB Query types
var OxlaPool = sync.Pool{
	New: func() interface{} {
		return &Oxla{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			Table:            make([]byte, 0, 1024),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

// NewOxla returns a new Oxla Query instance
func NewOxla() *Oxla {
	return OxlaPool.Get().(*Oxla)
}

// GetID returns the ID of this Query
func (q *Oxla) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *Oxla) SetID(n uint64) {
	q.id = n
}

// String produces a debug-ready description of a Query.
func (q *Oxla) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Table: %s, Query: %s", q.HumanLabel, q.HumanDescription, q.Table, q.SqlQuery)
}

// HumanLabelName returns the human readable name of this Query
func (q *Oxla) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *Oxla) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *Oxla) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0

	q.Table = q.Table[:0]
	q.SqlQuery = q.SqlQuery[:0]

	OxlaPool.Put(q)
}
