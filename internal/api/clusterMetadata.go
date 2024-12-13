package api

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
)

const (
	TOPIC_RECORD         byte = 2
	PARTITION_RECORD     byte = 3
	FEATURE_LEVEL_RECORD byte = 12
)

func getExistingTopics() []Topic {
	data, err := readClusterMetadata()
	if err != nil {
		log.Fatal("error reading __cluster_metadata", err)
	}

	topics := make([]Topic, 0)
	partitions := make([]Partition, 0)

	pos := 0
	for pos < len(data) {
		pos += 8 // Base Offset
		batchLength := binary.BigEndian.Uint32(data[pos:])
		pos += 4 // Batch Length

		batchPos := pos + 45 // Currently irrelevant data
		recordsLength := binary.BigEndian.Uint32(data[batchPos:])
		batchPos += 4 // Records length

		for i := 0; i < int(recordsLength); i++ {
			batchPos += 7 // Currently irrelevant data
			batchType := data[batchPos]
			batchPos += 2 // Currently irrelevant data

			switch batchType {
			case FEATURE_LEVEL_RECORD:
				nameLength := int(data[batchPos])
				fmt.Println("Feature level record name:", string(data[batchPos:batchPos+nameLength]))
				batchPos += nameLength + 4

			case TOPIC_RECORD:
				topic := Topic{errorCode: 0, isInternal: 0}
				nameLength := int(data[batchPos])
				topic.name = strings.Trim(string(data[batchPos : batchPos+nameLength]), string(rune(0x04)))
				batchPos += nameLength
				topic.uuid = data[batchPos : batchPos+16]
				batchPos += 20 // uuid + currently irrelevant data
				topics = append(topics, topic)

			case PARTITION_RECORD:
				partition := Partition{ID: binary.BigEndian.Uint32(data[batchPos:]), errorCode: 0}
				batchPos += 4
				partition.topicUuid = data[batchPos : batchPos+16]
				partitions = append(partitions, partition)
				batchPos += 61 // uuid + currently irrelevant data
			}

		}

		pos += int(batchLength)
	}

	for _, partition := range partitions {
		topicIndex := slices.IndexFunc(topics, func(t Topic) bool { return bytes.Equal(partition.topicUuid, t.uuid) })
		if topicIndex != -1 {
			topics[topicIndex].partitions = append(topics[topicIndex].partitions, partition)
		}
	}

	return topics
}

func readClusterMetadata() ([]byte, error) {
	content, err := os.ReadFile("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	if err != nil {
		return []byte{}, err
	}

	return content, nil
}
