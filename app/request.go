package main

import (
	"encoding/binary"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/internal/api"
)

func parseRequest(request []byte) (api.Deserializable, error) {
	apiKey := binary.BigEndian.Uint16(request[4:6])
	var req api.Deserializable = nil
	switch apiKey {
	case uint16(api.API_VERSIONS):
		req = &api.APIVersionsReq{}
	case uint16(api.DESCRIBE_TOPIC_PARTITIONS):
		req = &api.DescribeTopicPartitionsReq{}
	case uint16(api.FETCH):
		req = &api.FetchReq{}
	default:
		return nil, fmt.Errorf("unknown api key: %v", apiKey)
	}

	req.Deserialize(request)
	return req, nil
}