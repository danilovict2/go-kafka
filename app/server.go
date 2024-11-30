package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
)


func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	for {
		buf := make([]byte, 1024)
		_, err = conn.Read(buf)
		if errors.Is(err, io.EOF) {
			fmt.Println("Client closed connections at:", conn.RemoteAddr())
			break
		} else if err != nil {
			fmt.Println(err)
			break
		}

		requestMessage := ParseRequestMessage(buf)
		responseMessage := BuildResponseMessage(requestMessage)
		conn.Write(responseMessage.Marshal())
	}
}
