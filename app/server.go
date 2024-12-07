package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
)

type Server struct {
	listener net.Listener
}

func NewServer() *Server {
	return &Server{}
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	server := NewServer()
	server.listener = l
	server.Accept()
}

func (s *Server) Accept() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go s.Handle(conn)
	}
}

func (s *Server) Handle(conn net.Conn) {
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if errors.Is(err, io.EOF) {
			fmt.Println("Client closed connections at:", conn.RemoteAddr())
			break
		} else if err != nil {
			fmt.Println(err)
			break
		}

		requestMessage := ParseRequestMessage(buf)
		responseMessage := BuildResponseMessage(requestMessage)
		fmt.Println(responseMessage, responseMessage.Marshal())
		Send(conn, responseMessage.Marshal())
	}
}

func Send(conn net.Conn, message []byte) {
	binary.Write(conn, binary.BigEndian, int32(len(message)))
	binary.Write(conn, binary.BigEndian, message)
}
