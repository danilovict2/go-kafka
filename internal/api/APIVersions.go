package api

import "encoding/binary"

type APIVersionsReq struct {
	size         uint32
	apiKey       uint16
	apiVersion   uint16
	corelationID uint32
}

func (req *APIVersionsReq) Deserialize(request []byte) {
	req.size = binary.BigEndian.Uint32(request[0:4])
	req.apiKey = binary.BigEndian.Uint16(request[4:6])
	req.apiVersion = binary.BigEndian.Uint16(request[6:8])
	req.corelationID = binary.BigEndian.Uint32(request[8:12])
}

type APIVersionsResp struct {
	corelationID   uint32
	errorCode      API_ERROR_CODE
	numOfAPIKeys   uint8
	APIVersions    []APIVersions
	throttleTimeMS uint32
}

type APIVersions struct {
	apiKey     API_KEY
	minVersion uint16
	maxVersion uint16
}

func (resp *APIVersionsResp) Serialize() []byte {
	ret := make([]byte, 7)

	binary.BigEndian.PutUint32(ret, resp.corelationID)
	binary.BigEndian.PutUint16(ret[4:], uint16(resp.errorCode))

	ret[6] = resp.numOfAPIKeys + 1

	for i := 0; i < int(resp.numOfAPIKeys); i++ {
		apiKey := make([]byte, 6)
		binary.BigEndian.PutUint16(apiKey, uint16(resp.APIVersions[i].apiKey))
		binary.BigEndian.PutUint16(apiKey[2:], resp.APIVersions[i].minVersion)
		binary.BigEndian.PutUint16(apiKey[4:], resp.APIVersions[i].maxVersion)

		ret = append(ret, apiKey...)
		ret = append(ret, 0) // _tagged_fields
	}

	throttleTimeMS := make([]byte, 4)
	binary.BigEndian.PutUint32(throttleTimeMS, resp.throttleTimeMS)

	ret = append(ret, throttleTimeMS...)
	ret = append(ret, 0) // _tagged_fields

	return ret
}

func HandleApiVersionsReq(req *APIVersionsReq) *APIVersionsResp {
	var errorCode API_ERROR_CODE = 0
	if req.apiVersion > 4 {
		errorCode = UNSUPPORTED_VERSION_ERROR_CODE
	}

	return &APIVersionsResp{
		corelationID:   req.corelationID,
		errorCode:      errorCode,
		numOfAPIKeys:   3,
		APIVersions: []APIVersions{
			{
				apiKey: API_VERSIONS,
				minVersion: 3,
				maxVersion: 4,
			},
			{
				apiKey: DESCRIBE_TOPIC_PARTITIONS,
				minVersion: 0,
				maxVersion: 0,
			},
			{
				apiKey: FETCH,
				minVersion: 0,
				maxVersion: 16,
			},
		},
		throttleTimeMS: 0,
	}
}
