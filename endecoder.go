package gooutstore

import "encoding/json"

func encode(m IOutboxMessage) ([]byte, error) {
	return json.Marshal(m)
}

func decode[T IOutboxMessage](data []byte) (T, error) {
	var m T
	err := json.Unmarshal(data, &m)

	return m, err
}
