package main

import "encoding/binary"

type RequestMessage struct {
	size         uint32
	apiKey       uint16
	apiVersion   uint16
	corelationID uint32
}

func ParseRequestMessage(request []byte) RequestMessage {
	ret := RequestMessage{
		size: binary.BigEndian.Uint32(request[0:4]),
		apiKey: binary.BigEndian.Uint16(request[4:6]),
		apiVersion: binary.BigEndian.Uint16(request[6:8]),
		corelationID: binary.BigEndian.Uint32(request[8:12]),
	}

	return ret
}
