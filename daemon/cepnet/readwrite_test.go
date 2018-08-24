package cepnet

import (
	"bytes"
	"reflect"
	"testing"
)

func testReadWriteRt(t *testing.T, msg interface{}) {
	var bytes bytes.Buffer
	writer := NewWriter(&bytes)
	err := writer.WriteMessage(44, msg)
	if err != nil {
		t.Errorf("round trip write of %T failed: %s", msg, err)
		return
	}

	if bytes.Len() == 0 {
		t.Errorf("round trip of %T did not write anything", msg)
		return
	}

	reader := NewReader(&bytes)
	channelID, decMsg, err := reader.ReadMessage()
	if err != nil {
		t.Errorf("round trip read of %T failed: %s", msg, err)
		return
	}

	if channelID != 44 {
		t.Errorf("round trip of %T channel id failed", msg)
		return
	}

	if !reflect.DeepEqual(msg, decMsg) {
		t.Errorf("round trip of %T failed", msg)
		t.Errorf("  expected: %+v", msg)
		t.Errorf("  recieved: %+v", decMsg)
		return
	}
}

func TestReadWrite(t *testing.T) {
	// Control Messages
	testReadWriteRt(t, &OpenChannelMessage{
		ChannelID: 85,
	})

	testReadWriteRt(t, &CloseChannelMessage{
		ChannelID: 19,
	})

	// Generic Messages
	testReadWriteRt(t, &SuccessMessage{})

	testReadWriteRt(t, &ErrorMessage{
		Code:    99,
		Message: "i hate errors",
	})

	// Normal Messages
	testReadWriteRt(t, &StreamAddFilterMessage{
		FilterID: 9439,
		Filter:   []byte("sdljkglkjsdg"),
	})

	testReadWriteRt(t, &StreamRemoveFilterMessage{
		FilterID: 9439,
	})

	testReadWriteRt(t, &StreamStartMessage{})

	testReadWriteRt(t, &StreamRecoverMessage{
		InstanceID: "thisisatest",
		FromIndex:  14,
	})

	testReadWriteRt(t, &StreamStopMessage{})

	testReadWriteRt(t, &StreamStartedMessage{
		InstanceID: "iamaninstanceid",
	})

	// Push Messages
	testReadWriteRt(t, &PushStreamAddFilterMessage{
		FilterID: 938,
	})

	testReadWriteRt(t, &PushStreamRemoveFilterMessage{
		FilterID: 9382,
	})

	testReadWriteRt(t, &PushStreamItemMessage{
		EventIndex: 9284983,
		Data:       []byte("this is some test data"),
	})

	testReadWriteRt(t, &PushStreamAdvanceMessage{
		EventIndex: 928983,
	})

	testReadWriteRt(t, &PushStreamEndMessage{
		Reason:  19,
		Message: "hello world",
	})

}
