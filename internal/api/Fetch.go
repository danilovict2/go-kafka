package api

import (
	"encoding/binary"
)

type FetchReq struct {
	size         uint32
	apiKey       uint16
	apiVersion   uint16
	corelationID uint32
}

func (req *FetchReq) Deserialize(request []byte) {
	req.size = binary.BigEndian.Uint32(request[0:4])
	req.apiKey = binary.BigEndian.Uint16(request[4:6])
	req.apiVersion = binary.BigEndian.Uint16(request[6:8])
	req.corelationID = binary.BigEndian.Uint32(request[8:12])
}

type FetchResp struct {
	corelationID   uint32
	throttleTimeMS uint32
	errorCode      API_ERROR_CODE
	sessionID      uint32
	responses      []Topic
}

func (resp *FetchResp) Serialize() []byte {
	ret := make([]byte, 15)
	binary.BigEndian.PutUint32(ret, resp.corelationID)
	ret[4] = 0 // _tagged_fields

	binary.BigEndian.PutUint32(ret[5:], resp.throttleTimeMS)
	binary.BigEndian.PutUint16(ret[9:], uint16(resp.errorCode))
	binary.BigEndian.PutUint32(ret[11:], resp.sessionID)
	ret = append(ret, byte(len(resp.responses)+1))
	ret = append(ret, 0)                           // _tagged_fields

	return ret
}

func HandleFetchReq(req *FetchReq) *FetchResp {
	resp := FetchResp{
		corelationID:   req.corelationID,
		throttleTimeMS: 0,
		errorCode:      0,
		sessionID:      0,
		responses:      make([]Topic, 0),
	}

	return &resp
}
