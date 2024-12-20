package api

import (
	"encoding/binary"
)

type RecordBatch struct {
	baseOffset           uint64
	length               uint32
	partitionLeaderEpoch uint32
	magicByte            byte
	crc                  uint32
	attributes           uint16
	lastOffsetDelta      uint32
	baseTimestamp        uint64
	maxTimestamp         uint64
	producerID           uint64
	producerEpoch        uint16
	baseSequence         uint32
	recordsLength        uint32
	records              []byte
}

func (rb *RecordBatch) Serialize() []byte {
	serializedBatch := make([]byte, 61)

	binary.BigEndian.PutUint64(serializedBatch, rb.baseOffset)
	binary.BigEndian.PutUint32(serializedBatch[8:], rb.length)
	binary.BigEndian.PutUint32(serializedBatch[12:], rb.partitionLeaderEpoch)
	serializedBatch[16] = rb.magicByte
	binary.BigEndian.PutUint32(serializedBatch[17:], rb.crc)
	binary.BigEndian.PutUint16(serializedBatch[21:], rb.attributes)
	binary.BigEndian.PutUint32(serializedBatch[23:], rb.lastOffsetDelta)
	binary.BigEndian.PutUint64(serializedBatch[27:], rb.baseTimestamp)
	binary.BigEndian.PutUint64(serializedBatch[35:], rb.maxTimestamp)
	binary.BigEndian.PutUint64(serializedBatch[43:], rb.producerID)
	binary.BigEndian.PutUint16(serializedBatch[51:], rb.producerEpoch)
	binary.BigEndian.PutUint32(serializedBatch[53:], rb.baseSequence)
	binary.BigEndian.PutUint32(serializedBatch[57:], rb.recordsLength)
	serializedBatch = append(serializedBatch, rb.records...)

	return serializedBatch
}
