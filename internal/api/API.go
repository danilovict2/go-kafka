package api

type API_KEY uint16
type API_ERROR_CODE uint16

const (
	API_VERSIONS              API_KEY = 18
	DESCRIBE_TOPIC_PARTITIONS API_KEY = 75
)

const (
	UNKNOWN_TOPIC_OR_PARTITION     API_ERROR_CODE = 3
	UNSUPPORTED_VERSION_ERROR_CODE API_ERROR_CODE = 35
)

type Serializable interface {
	Serialize() []byte
}

type Deserializable interface {
	Deserialize([]byte)
}