package api

import (
	"encoding/binary"
)

type DescribeTopicPartitionsReq struct {
	size         uint32
	apiKey       uint16
	apiVersion   uint16
	corelationID uint32
	topics       []string
	clientID     string
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
		req.topics = append(req.topics, string(topicName))
		position += uint16(topicNameLength) + 2
	}
}

type DescribeTopicPartitionsResp struct {
	corelationID   uint32
	throttleTimeMS uint32
	numOfTopics    uint8
	topics         []TopicDetails
}

type TopicDetails struct {
	errorCode            API_ERROR_CODE
	name                 string
	ID                   string
	isInternal           uint8
	partitions           []struct{}
}

func (resp *DescribeTopicPartitionsResp) Serialize() []byte {
	ret := make([]byte, 10)
	binary.BigEndian.PutUint32(ret, resp.corelationID)
	ret[4] = 0 // _tagged_fields

	binary.BigEndian.PutUint32(ret[5:], resp.throttleTimeMS)
	ret[9] = resp.numOfTopics + 1

	for _, topic := range resp.topics {
		serializedTopic := make([]byte, 2)

		binary.BigEndian.PutUint16(serializedTopic, uint16(topic.errorCode))
		serializedTopic = append(serializedTopic, byte(len(topic.name)+1))
		serializedTopic = append(serializedTopic, topic.name...)
		serializedTopic = append(serializedTopic, topic.ID...)
		serializedTopic = append(serializedTopic, 0, 0, 0, 0) //  Topic Authorized Operations
		serializedTopic = append(serializedTopic, topic.isInternal)
		serializedTopic = append(serializedTopic, 1) // Empty partitions array
		serializedTopic = append(serializedTopic, 0) // _tagged_fields

		ret = append(ret, serializedTopic...)
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

	for _, topicName := range req.topics {
		topic := TopicDetails{
			errorCode:            UNKNOWN_TOPIC_OR_PARTITION,
			name:                 topicName,
			ID:                   string([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			isInternal:           0,
			partitions:           make([]struct{}, 0),
		}

		resp.topics = append(resp.topics, topic)
	}

	resp.numOfTopics = uint8(len(resp.topics))
	return &resp
}
