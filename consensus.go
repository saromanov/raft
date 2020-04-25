package raft

import (
	"sync"
)

// Consensus provides implementation of consensus module
type Consensus struct {
	mu sync.Mutex
	id int

	peers  []int
	server *Server
}
