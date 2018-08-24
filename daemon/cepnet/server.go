package cepnet

import (
	"net"
)

type Server struct {
	listener net.Listener
}

func (srv *Server) Accept() (*Conn, error) {
	conn, err := srv.listener.Accept()
	if err != nil {
		return nil, err
	}

	return wrapConn(conn)
}

func (srv *Server) Close() error {
	return srv.listener.Close()
}

func (srv *Server) Addr() net.Addr {
	return srv.listener.Addr()
}

func Listen(laddr string) (*Server, error) {
	ln, err := net.Listen("tcp", laddr)
	if err != nil {
		return nil, err
	}

	server := &Server{
		listener: ln,
	}

	return server, nil
}
