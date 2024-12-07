package main

import "encoding/binary"

type ResponseMessage struct {
	corelationID   uint32
	errorCode      uint16
	numOfAPIKeys   uint8
	APIKeys        []APIKey
	throttleTimeMS uint32
}

type APIKey interface {
	Marshal() []byte
}

type ApiVersions struct {
	apiKey     uint16
	minVersion uint16
	maxVersion uint16
}

type DescribeTopicPartitions struct {
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
		corelationID:   request.corelationID,
		errorCode:      errorCode,
		numOfAPIKeys:   2,
		APIKeys: []APIKey{
			ApiVersions{
				apiKey: 18,
				minVersion: 3,
				maxVersion: 4,
			},
			DescribeTopicPartitions{
				apiKey: 75,
				minVersion: 0,
				maxVersion: 0,
			},
		},
		throttleTimeMS: 0,
	}
}

func (r ResponseMessage) Marshal() []byte {
	ret := make([]byte, 7)

	binary.BigEndian.PutUint32(ret, r.corelationID)
	binary.BigEndian.PutUint16(ret[4:], r.errorCode)

	ret[6] = r.numOfAPIKeys + 1
	for i := 0; i < int(r.numOfAPIKeys); i++ {
		apiKey := r.APIKeys[i].Marshal()
		ret = append(ret, apiKey...)
		ret = append(ret, 0) // _tagged_fields
	}

	throttleTimeMS := make([]byte, 4)
	binary.BigEndian.PutUint32(throttleTimeMS, r.throttleTimeMS)

	ret = append(ret, throttleTimeMS...)
	ret = append(ret, 0) // _tagged_fields

	return ret
}

func (apiKey ApiVersions) Marshal() []byte {
	ret := make([]byte, 6)
	binary.BigEndian.PutUint16(ret, apiKey.apiKey)
	binary.BigEndian.PutUint16(ret[2:], apiKey.minVersion)
	binary.BigEndian.PutUint16(ret[4:], apiKey.maxVersion)

	return ret
}

func (apiKey DescribeTopicPartitions) Marshal() []byte {
	ret := make([]byte, 6)
	binary.BigEndian.PutUint16(ret, apiKey.apiKey)
	binary.BigEndian.PutUint16(ret[2:], apiKey.minVersion)
	binary.BigEndian.PutUint16(ret[4:], apiKey.maxVersion)

	return ret
}