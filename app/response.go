package main

import (
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/internal/api"
)

func parseResponse(req api.Deserializable) (api.Serializable, error) {
	var resp api.Serializable = nil

	switch request := req.(type) {
	case *api.APIVersionsReq:
		resp = api.HandleApiVersionsReq(request);
	case *api.DescribeTopicPartitionsReq:
		resp = api.HandleDescribeTopicPartitionsReq(request)
	default:
		return nil, fmt.Errorf("invalid request type: %T", resp)
	}

	return resp, nil
}