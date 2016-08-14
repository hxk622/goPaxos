package common

import "gopkg.in/vmihailenco/msgpack.v2"

//decode
func FromMsgPack(data []byte, obj interface{}) error {
	return msgpack.Unmarshal(data, &obj)
}

//encode
func ToMsgPack(obj interface{}) ([]byte, error) {
	data, err := msgpack.Marshal(obj)
	return data, err
}