package main

import "encoding/binary"

type ResponseMessage struct {
	size         uint32
	corelationID uint32
	errorCode    uint16
}

func BuildResponseMessage(request RequestMessage) ResponseMessage {
	var errorCode uint16 = 0
	if request.apiVersion > 4 {
		errorCode = 35
	}

	return ResponseMessage{
		size: 0,
		corelationID: request.corelationID,
		errorCode: errorCode,
	}
}

func (r ResponseMessage) Marshal() []byte {
	size := make([]byte, 4)
	corelationID := make([]byte, 4)
	errorCode := make([]byte, 2)

	binary.BigEndian.PutUint32(size, r.size)
	binary.BigEndian.PutUint32(corelationID, r.corelationID)
	binary.BigEndian.PutUint16(errorCode, r.errorCode)

	ret := make([]byte, 0)
	ret = append(ret, size...)
	ret = append(ret, corelationID...)
	ret = append(ret, errorCode...)
	
	return ret
}
