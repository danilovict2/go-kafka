package api

import "encoding/binary"

type Topic struct {
	errorCode  API_ERROR_CODE
	name       string
	uuid       []byte
	isInternal uint8
	partitions []Partition
}

type Partition struct {
	errorCode API_ERROR_CODE
	ID        uint32
	topicUuid []byte
}

func (topic *Topic) SerializeForDescribeTopicPartitions() []byte {
	serializedTopic := make([]byte, 2)

	binary.BigEndian.PutUint16(serializedTopic, uint16(topic.errorCode))
	serializedTopic = append(serializedTopic, byte(len(topic.name)+1))
	serializedTopic = append(serializedTopic, topic.name...)
	serializedTopic = append(serializedTopic, topic.uuid...)
	serializedTopic = append(serializedTopic, topic.isInternal)
	serializedTopic = append(serializedTopic, byte(len(topic.partitions)+1))

	for _, partition := range topic.partitions {
		serializedTopic = append(serializedTopic, partition.SerializeForDescribeTopicPartitions()...)
	}

	serializedTopic = append(serializedTopic, 0, 0, 0, 0) //  Topic Authorized Operations
	serializedTopic = append(serializedTopic, 0)          // _tagged_fields

	return serializedTopic
}

func (partition *Partition) SerializeForDescribeTopicPartitions() []byte {
	serializedPartition := make([]byte, 6)

	binary.BigEndian.PutUint16(serializedPartition, uint16(partition.errorCode))
	binary.BigEndian.PutUint32(serializedPartition[2:], partition.ID)
	serializedPartition = append(serializedPartition, 0, 0, 0, 1)    // Leader ID
	serializedPartition = append(serializedPartition, 0, 0, 0, 0)    // Leader Epoch
	serializedPartition = append(serializedPartition, 2, 0, 0, 0, 1) // Replica Nodes
	serializedPartition = append(serializedPartition, 2, 0, 0, 0, 1) // ISR Nodes
	serializedPartition = append(serializedPartition, 1)             // Eligible Leader Replicas
	serializedPartition = append(serializedPartition, 1)             // Last Known ELR
	serializedPartition = append(serializedPartition, 1)             // Offline Replicas
	serializedPartition = append(serializedPartition, 0)             // _tagged_fields

	return serializedPartition
}

func (topic *Topic) SerializeForFetch() []byte {
	serializedTopic := make([]byte, 0)

	serializedTopic = append(serializedTopic, topic.uuid...)
	serializedTopic = append(serializedTopic, byte(len(topic.partitions)+1))

	for _, partition := range topic.partitions {
		serializedTopic = append(serializedTopic, partition.SerializeForFetch()...)
	}

	serializedTopic = append(serializedTopic, 0) // _tagged_fields

	return serializedTopic
}

func (partition *Partition) SerializeForFetch() []byte {
	serializedPartition := make([]byte, 6)

	binary.BigEndian.PutUint32(serializedPartition, partition.ID)
	binary.BigEndian.PutUint16(serializedPartition[4:], uint16(partition.errorCode))
	serializedPartition = append(serializedPartition, 0, 0, 0, 0, 0, 0, 0, 0) // high_watermark
	serializedPartition = append(serializedPartition, 0, 0, 0, 0, 0, 0, 0, 0) // last_stable_offset
	serializedPartition = append(serializedPartition, 0, 0, 0, 0, 0, 0, 0, 0) // log_start_offset
	serializedPartition = append(serializedPartition, 0)                      // _tagged_fields
	serializedPartition = append(serializedPartition, 0, 0, 0, 0)             // preferred_read_replica
	serializedPartition = append(serializedPartition, 1)                      // records
	serializedPartition = append(serializedPartition, 0)                      // _tagged_fields

	return serializedPartition
}

func NewUnknownTopic(topicName string) Topic {
	return Topic{
		errorCode:  UNKNOWN_TOPIC_OR_PARTITION,
		name:       topicName,
		uuid:       []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		isInternal: 0,
		partitions: make([]Partition, 0),
	}
}
