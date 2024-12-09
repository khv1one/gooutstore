package gooutstore

import (
	"reflect"
	"testing"
)

type testEndecodeMessage struct {
	TestInt    int
	TestString string
}

func (m testEndecodeMessage) Key() string {
	return "test_key"
}

func (m testEndecodeMessage) Type() string {
	return "test_type"
}

func Test_encode(t *testing.T) {
	tests := []struct {
		name    string
		arg     IOutboxMessage
		want    []byte
		wantErr bool
	}{
		{
			name:    "encode test",
			arg:     testEndecodeMessage{1, "field"},
			want:    []byte("{\"TestInt\":1,\"TestString\":\"field\"}"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encode(tt.arg)
			if (err != nil) != tt.wantErr {
				t.Errorf("encode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("encode() = %v, want %v", got, tt.want)
			}
		})
	}
}
