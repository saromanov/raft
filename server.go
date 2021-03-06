// Server container for a Raft Consensus Module. Exposes Raft to the network
// and enables RPCs between Raft peers.
//
// Eli Bendersky [https://eli.thegreenplace.net]
// This code is in the public domain.
package raft

import (
	"log"
	"net"
	"net/rpc"
	"sync"
)

type Server struct {
	mu sync.Mutex

	serverId int
	peerIds  []int

	ready <-chan interface{}
	quit  chan struct{}
	wg    sync.WaitGroup

	cm       *ConsensusModule
	rpcProxy *RPCProxy

	rpcServer *rpc.Server
	listener  net.Listener

	peers map[int]*rpc.Client
}

// New server creates a new one server
func NewServer(id int, peerIds []int, ready <-chan interface{}) *Server {
	s := new(Server)
	s.quit = make(chan struct{})
	return &Server{
		serverId: id,
		peerIds:  peerIds,
		ready:    ready,
		quit:     make(chan struct{}),
	}
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.cm = NewConsensusModule(s.serverId, s.peerIds, s, s.ready)

	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{cm: s.cm}
	s.rpcServer.RegisterName("ConsensusModule", s.rpcProxy)

	var err error
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.serverId, s.listener.Addr())
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			s.wg.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

// DisconnectAll provides disconnecting of all clients
func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peers {
		if s.peers[id] != nil {
			s.peers[id].Close()
			s.peers[id] = nil
		}
	}
}

// Shutdown provides shutdownling of the server
func (s *Server) Shutdown() {
	s.cm.Stop()
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

// GetListenAddr returns net address
func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

// ConnectToPeer provides connecting to the peer
func (s *Server) ConnectToPeer(peerID int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peers[peerID] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peers[peerID] = client
	}
	return nil
}

// DisconnectPeer disconnects this server from the peer identified by peerId.
func (s *Server) DisconnectPeer(peerID int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peers[peerID] != nil {
		err := s.peers[peerID].Close()
		s.peers[peerID] = nil
		return err
	}
	return nil
}
