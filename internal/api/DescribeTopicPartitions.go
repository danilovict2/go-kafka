package api

import (
	"encoding/binary"
	"slices"
)

type DescribeTopicPartitionsReq struct {
	size         uint32
	apiKey       uint16
	apiVersion   uint16
	corelationID uint32
	clientID     string
	topicNames   []string
}

func (req *DescribeTopicPartitionsReq) Deserialize(request []byte) {
	req.size = binary.BigEndian.Uint32(request[0:4])
	req.apiKey = binary.BigEndian.Uint16(request[4:6])
	req.apiVersion = binary.BigEndian.Uint16(request[6:8])
	req.corelationID = binary.BigEndian.Uint32(request[8:12])

	clientIDLength := binary.BigEndian.Uint16(request[12:])
	req.clientID = string(request[14 : 13+clientIDLength])
	numOfTopics := request[15+clientIDLength] - 1
	position := 16 + clientIDLength

	for i := 0; i < int(numOfTopics); i++ {
		topicNameLength := request[position] - 1
		topicName := request[position+1 : position+1+uint16(topicNameLength)]
		req.topicNames = append(req.topicNames, string(topicName))
		position += uint16(topicNameLength) + 2
	}
}

type DescribeTopicPartitionsResp struct {
	corelationID   uint32
	throttleTimeMS uint32
	numOfTopics    uint8
	topics         []Topic
}

func (resp *DescribeTopicPartitionsResp) Serialize() []byte {
	ret := make([]byte, 10)
	binary.BigEndian.PutUint32(ret, resp.corelationID)
	ret[4] = 0 // _tagged_fields

	binary.BigEndian.PutUint32(ret[5:], resp.throttleTimeMS)
	ret[9] = resp.numOfTopics + 1

	for _, topic := range resp.topics {
		ret = append(ret, topic.SerializeForDescribeTopicPartitions()...)
	}

	ret = append(ret, 0xFF) // Next Cursor
	ret = append(ret, 0)    // _tagged_fields

	return ret
}

func HandleDescribeTopicPartitionsReq(req *DescribeTopicPartitionsReq) *DescribeTopicPartitionsResp {
	resp := DescribeTopicPartitionsResp{
		corelationID:   req.corelationID,
		throttleTimeMS: 0,
	}

	topics := getClusterMetadataLogs("__cluster_metadata", 0).topics

	for _, topicName := range req.topicNames {
		var topic Topic
		if idx := slices.IndexFunc(topics, func(t Topic) bool {return t.name == topicName}); idx != -1 {
			topic = topics[idx]
		} else {
			topic = NewUnknownTopic(topicName)
		}
		
		resp.topics = append(resp.topics, topic)
	}

	resp.numOfTopics = uint8(len(resp.topics))
	return &resp
}
