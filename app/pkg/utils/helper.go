package utils

import "encoding/json"

func CompressJSON(obj any) []byte{
	raw, _ := json.Marshal(obj)
	return raw
}