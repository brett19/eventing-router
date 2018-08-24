package cepnet

type Message interface{}

const (
	// Normal Messages (0x0000 upwards)
	cmdSuccess            = uint16(0x0001)
	cmdError              = uint16(0x0002)
	cmdStreamAddFilter    = uint16(0x0003)
	cmdStreamRemoveFilter = uint16(0x0004)
	cmdStreamStart        = uint16(0x0005)
	cmdStreamRecover      = uint16(0x0006)
	cmdStreamStop         = uint16(0x0007)
	cmdStreamStarted      = uint16(0x0008)

	// Temporary Messages (0x7000 and up)

	// Push Messages (0x8000 and up, ie: high bit set)
	cmdPushStreamAddFilter    = uint16(0x8001)
	cmdPushStreamRemoveFilter = uint16(0x8002)
	cmdPushStreamItem         = uint16(0x8003)
	cmdPushStreamAdvance      = uint16(0x8004)
	cmdPushStreamEnd          = uint16(0x8005)

	// Control Messages (0x7fff downwards)
	cmdOpenChannel  = uint16(0x7ffe)
	cmdCloseChannel = uint16(0x7ffd)
	cmdFlowAck      = uint16(0x7ffc)
)

type SuccessMessage struct {
}

// TODO(brett19): This message is used by the control system,
// but is a non-control message!
type ErrorMessage struct {
	Code    uint32
	Message string
}

type StreamAddFilterMessage struct {
	FilterID uint16
	Filter   []byte
}

type StreamRemoveFilterMessage struct {
	FilterID uint16
}

type StreamStartMessage struct {
}

type StreamRecoverMessage struct {
	InstanceID string
	FromIndex  uint64
}

type StreamStopMessage struct {
}

type StreamStartedMessage struct {
	InstanceID string
}

type PushStreamAddFilterMessage struct {
	FilterID uint16
}

type PushStreamRemoveFilterMessage struct {
	FilterID uint16
}

type PushStreamItemMessage struct {
	EventIndex uint64
	Data       []byte
}

type PushStreamAdvanceMessage struct {
	EventIndex uint64
}

type PushStreamEndMessage struct {
	Reason  uint32
	Message string
}

type OpenChannelMessage struct {
	ChannelID uint16
}

type CloseChannelMessage struct {
	ChannelID uint16
}
