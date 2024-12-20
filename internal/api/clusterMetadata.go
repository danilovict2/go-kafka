package api

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
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

type ClusterMetadata struct {
	topics     []Topic
	partitions []Partition
	batches    []RecordBatch
}

func getClusterMetadataLogs(topicName string, partitionID int) ClusterMetadata {
	data, err := readClusterMetadata(topicName, partitionID)
	if err != nil {
		log.Fatal("error reading __cluster_metadata", err)
	}

	topics := make([]Topic, 0)
	partitions := make([]Partition, 0)
	batches := make([]RecordBatch, 0)

	pos := 0
	for pos < len(data) {
		batch := NewRecordBatch(data, pos)

		pos += 12            // Batch offset and Batch Length
		batchPos := pos + 49 // Skip batch info

		for i := 0; i < int(batch.recordsLength); i++ {
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
				topic.name = strings.Trim(string(data[batchPos:batchPos+nameLength]), string(rune(0x04)))
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

		batches = append(batches, batch)
		pos += int(batch.length)
	}

	for _, partition := range partitions {
		topicIndex := slices.IndexFunc(topics, func(t Topic) bool { return bytes.Equal(partition.topicUuid, t.uuid) })
		if topicIndex != -1 {
			topics[topicIndex].partitions = append(topics[topicIndex].partitions, partition)
		}
	}

	return ClusterMetadata{
		topics: topics,
		partitions: partitions,
		batches: batches,
	}
}

func readClusterMetadata(topicName string, partitionID int) ([]byte, error) {
	data, err := os.ReadFile(fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log", topicName, partitionID))
	if err != nil {
		return []byte{}, err
	}

	return data, nil
}

func NewRecordBatch(data []byte, offset int) RecordBatch {
	pos := 0
	batch := RecordBatch{}
	batch.baseOffset = binary.BigEndian.Uint64(data[offset:])
	pos += 8
	batch.length = binary.BigEndian.Uint32(data[pos+offset:])
	pos += 4
	batch.partitionLeaderEpoch = binary.BigEndian.Uint32(data[pos+offset:])
	pos += 4
	batch.magicByte = data[pos]
	pos += 5 // magic byte + crc
	afterCrcPos := pos
	batch.attributes = binary.BigEndian.Uint16(data[pos+offset:])
	pos += 2
	batch.lastOffsetDelta = binary.BigEndian.Uint32(data[pos+offset:])
	pos += 4
	batch.baseTimestamp = binary.BigEndian.Uint64(data[pos+offset:])
	pos += 8
	batch.maxTimestamp = binary.BigEndian.Uint64(data[pos+offset:])
	pos += 8
	batch.producerID = binary.BigEndian.Uint64(data[pos+offset:])
	pos += 8
	batch.producerEpoch = binary.BigEndian.Uint16(data[pos+offset:])
	pos += 2
	batch.baseSequence = binary.BigEndian.Uint32(data[pos+offset:])
	pos += 4
	batch.recordsLength = binary.BigEndian.Uint32(data[pos+offset:])
	pos += 4
	batch.records = append(batch.records, data[pos+offset:int(batch.length)+12+offset]...)
	batch.crc = crc32.Checksum(batch.Serialize()[afterCrcPos:], crc32.MakeTable(crc32.Castagnoli))

	return batch
}
