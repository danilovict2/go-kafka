package main

import "encoding/binary"

type ResponseMessage struct {
	corelationID   uint32
	errorCode      uint16
	numOfAPIKeys   uint8
	apiVersions    ApiVersionsResponse
	throttleTimeMS uint32
}

type ApiVersionsResponse struct {
	apiKey     uint16
	minVersion uint16
	maxVersion uint16
}

func BuildResponseMessage(request RequestMessage) ResponseMessage {
	var errorCode uint16 = 0
	if request.apiVersion > 4 {
		errorCode = 35
	}

	return ResponseMessage{
		corelationID: request.corelationID,
		errorCode:    errorCode,
		numOfAPIKeys: 2,
		apiVersions: ApiVersionsResponse{
			apiKey:     18,
			minVersion: 3,
			maxVersion: 4,
		},
		throttleTimeMS: 0,
	}
}

func (r ResponseMessage) Marshal() []byte {
	ret := make([]byte, 19)

	binary.BigEndian.PutUint32(ret, r.corelationID)
	binary.BigEndian.PutUint16(ret[4:], r.errorCode)

	ret[6] = r.numOfAPIKeys
	binary.BigEndian.PutUint16(ret[7:], r.apiVersions.apiKey)
	binary.BigEndian.PutUint16(ret[9:], r.apiVersions.minVersion)
	binary.BigEndian.PutUint16(ret[11:], r.apiVersions.maxVersion)

	ret[13] = 0 // _tagged_fields
	binary.BigEndian.PutUint32(ret[14:], r.throttleTimeMS)
	ret[18] = 0 // _tagged_fields

	return ret
}
