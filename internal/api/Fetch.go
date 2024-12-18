package api

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
)

type FetchReq struct {
	size         uint32
	apiKey       uint16
	apiVersion   uint16
	corelationID uint32
	clientID     string
	topics       []Topic
}

func (req *FetchReq) Deserialize(request []byte) {
	req.size = binary.BigEndian.Uint32(request[0:4])
	req.apiKey = binary.BigEndian.Uint16(request[4:6])
	req.apiVersion = binary.BigEndian.Uint16(request[6:8])
	req.corelationID = binary.BigEndian.Uint32(request[8:12])

	clientIDLength := binary.BigEndian.Uint16(request[12:])
	req.clientID = string(request[14 : 13+clientIDLength])
	pos := 15 + clientIDLength + 21 // clientID + currently irrelevant data
	numOfTopics := int(request[pos] - 1)
	pos += 1
	for i := 0; i < numOfTopics; i++ {
		topic := Topic{
			uuid: request[pos : pos+16],
		}

		pos += 16
		partitionsLength := int(request[pos] - 1)
		pos += 1
		
		for j := 0; j < partitionsLength; j++ {
			partition := Partition{
				ID:        binary.BigEndian.Uint32(request[pos:]),
				topicUuid: topic.uuid,
				errorCode: UNKNOWN_TOPIC,
			}
			
			pos += 33 // ID + currently irrelevant data
			topic.partitions = append(topic.partitions, partition)
		}

		req.topics = append(req.topics, topic)
	}
}

type FetchResp struct {
	corelationID   uint32
	throttleTimeMS uint32
	errorCode      API_ERROR_CODE
	sessionID      uint32
	responses      []Topic
}

func (resp *FetchResp) Serialize() []byte {
	ret := make([]byte, 15)
	binary.BigEndian.PutUint32(ret, resp.corelationID)
	ret[4] = 0 // _tagged_fields

	binary.BigEndian.PutUint32(ret[5:], resp.throttleTimeMS)
	binary.BigEndian.PutUint16(ret[9:], uint16(resp.errorCode))
	binary.BigEndian.PutUint32(ret[11:], resp.sessionID)
	ret = append(ret, byte(len(resp.responses)+1))

	for _, topic := range resp.responses {
		ret = append(ret, topic.SerializeForFetch()...)
	}

	ret = append(ret, 0) // _tagged_fields

	fmt.Println(ret)
	return ret
}

func HandleFetchReq(req *FetchReq) *FetchResp {
	resp := FetchResp{
		corelationID:   req.corelationID,
		throttleTimeMS: 0,
		errorCode:      0,
		sessionID:      0,
		responses:      make([]Topic, 0),
	}

	existingTopics := getExistingTopics()

	for _, topic := range req.topics {
		var response Topic
		if topicIndex := slices.IndexFunc(existingTopics, func(t Topic) bool {return bytes.Equal(topic.uuid, t.uuid)}); topicIndex != -1 {
			response = existingTopics[topicIndex]
		} else {
			response = topic
		}
		
		resp.responses = append(resp.responses, response)
	}
	
	return &resp
}
