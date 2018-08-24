package cepnet

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"log"
)

var ErrBadMessage = errors.New("bad message")
var ErrUnknownMessage = errors.New("unknown message type")

type Reader struct {
	reader *bufio.Reader
}

func NewReader(reader io.Reader) *Reader {
	return &Reader{
		reader: bufio.NewReader(reader),
	}
}

type Writer struct {
	writer io.Writer
}

func NewWriter(writer io.Writer) *Writer {
	return &Writer{
		writer: writer,
	}
}

func (rdr *Reader) ReadMessage() (uint16, Message, error) {
	header := make([]byte, 8)
	_, err := io.ReadFull(rdr.reader, header)
	if err != nil {
		return 0, nil, err
	}

	pakLength := binary.BigEndian.Uint32(header[0:])
	payloadLength := pakLength - 8

	var payload []byte
	if payloadLength > 0 {
		payload = make([]byte, payloadLength)
		_, err := io.ReadFull(rdr.reader, payload)
		if err != nil {
			return 0, nil, err
		}
	}

	channelID := binary.BigEndian.Uint16(header[4:])
	msgType := binary.BigEndian.Uint16(header[6:])

	switch msgType {
	// Control Messages
	case cmdOpenChannel:
		if len(payload) != 2 {
			return 0, nil, ErrBadMessage
		}

		return channelID, &OpenChannelMessage{
			ChannelID: binary.BigEndian.Uint16(payload[0:]),
		}, nil
	case cmdCloseChannel:
		if len(payload) != 2 {
			return 0, nil, ErrBadMessage
		}

		return channelID, &CloseChannelMessage{
			ChannelID: binary.BigEndian.Uint16(payload[0:]),
		}, nil

		// Generic Messages
	case cmdSuccess:
		return channelID, &SuccessMessage{}, nil
	case cmdError:
		if len(payload) < 4 {
			return 0, nil, ErrBadMessage
		}

		return channelID, &ErrorMessage{
			Code:    binary.BigEndian.Uint32(payload[0:]),
			Message: string(payload[4:]),
		}, nil

		// Normal Messages
	case cmdStreamAddFilter:
		return channelID, &StreamAddFilterMessage{
			FilterID: binary.BigEndian.Uint16(payload[0:]),
			Filter:   payload[2:],
		}, nil
	case cmdStreamRemoveFilter:
		return channelID, &StreamRemoveFilterMessage{
			FilterID: binary.BigEndian.Uint16(payload[0:]),
		}, nil

	case cmdStreamStart:
		if len(payload) != 0 {
			return 0, nil, ErrBadMessage
		}

		return channelID, &StreamStartMessage{}, nil
	case cmdStreamRecover:
		if len(payload) < 8 {
			return 0, nil, ErrBadMessage
		}

		return channelID, &StreamRecoverMessage{
			FromIndex:  binary.BigEndian.Uint64(payload[0:]),
			InstanceID: string(payload[8:]),
		}, nil
	case cmdStreamStop:
		if len(payload) != 0 {
			return 0, nil, ErrBadMessage
		}

		return channelID, &StreamStopMessage{}, nil
	case cmdStreamStarted:
		if len(payload) == 0 {
			return 0, nil, ErrBadMessage
		}

		return channelID, &StreamStartedMessage{
			InstanceID: string(payload[0:]),
		}, nil

		// Push Messages
	case cmdPushStreamAddFilter:
		if len(payload) != 2 {
			return 0, nil, ErrBadMessage
		}

		return channelID, &PushStreamAddFilterMessage{
			FilterID: binary.BigEndian.Uint16(payload[0:]),
		}, nil
	case cmdPushStreamRemoveFilter:
		if len(payload) != 2 {
			return 0, nil, ErrBadMessage
		}

		return channelID, &PushStreamRemoveFilterMessage{
			FilterID: binary.BigEndian.Uint16(payload[0:]),
		}, nil

	case cmdPushStreamItem:
		if len(payload) < 8 {
			return 0, nil, ErrBadMessage
		}

		return channelID, &PushStreamItemMessage{
			EventIndex: binary.BigEndian.Uint64(payload[0:]),
			Data:       payload[8:],
		}, nil
	case cmdPushStreamAdvance:
		if len(payload) != 8 {
			return 0, nil, ErrBadMessage
		}

		return channelID, &PushStreamAdvanceMessage{
			EventIndex: binary.BigEndian.Uint64(payload[0:]),
		}, nil
	case cmdPushStreamEnd:
		if len(payload) < 4 {
			return 0, nil, ErrBadMessage
		}

		return channelID, &PushStreamEndMessage{
			Reason:  binary.BigEndian.Uint32(payload[0:]),
			Message: string(payload[4:]),
		}, nil

	}

	return channelID, nil, ErrUnknownMessage
}

func (wrt *Writer) WriteMessage(channelID uint16, msg Message) error {
	var data []byte

	switch msg := msg.(type) {
	// Control Messages
	case *OpenChannelMessage:
		data = make([]byte, 8+2)
		binary.BigEndian.PutUint16(data[6:], cmdOpenChannel)
		binary.BigEndian.PutUint16(data[8:], msg.ChannelID)
	case *CloseChannelMessage:
		data = make([]byte, 8+2)
		binary.BigEndian.PutUint16(data[6:], cmdCloseChannel)
		binary.BigEndian.PutUint16(data[8:], msg.ChannelID)

		// Generic Message
	case *SuccessMessage:
		data = make([]byte, 8)
		binary.BigEndian.PutUint16(data[6:], cmdSuccess)
	case *ErrorMessage:
		msgBytes := []byte(msg.Message)
		data = make([]byte, 8+4+len(msgBytes))
		binary.BigEndian.PutUint16(data[6:], cmdError)
		binary.BigEndian.PutUint32(data[8:], msg.Code)
		copy(data[12:], msgBytes)

		// Normal Messages
	case *StreamAddFilterMessage:
		data = make([]byte, 8+2+len(msg.Filter))
		binary.BigEndian.PutUint16(data[6:], cmdStreamAddFilter)
		binary.BigEndian.PutUint16(data[8:], msg.FilterID)
		copy(data[10:], msg.Filter)
	case *StreamRemoveFilterMessage:
		data = make([]byte, 8+2)
		binary.BigEndian.PutUint16(data[6:], cmdStreamRemoveFilter)
		binary.BigEndian.PutUint16(data[8:], msg.FilterID)
	case *StreamStartMessage:
		data = make([]byte, 8)
		binary.BigEndian.PutUint16(data[6:], cmdStreamStart)
	case *StreamRecoverMessage:
		instanceIDBytes := []byte(msg.InstanceID)
		data = make([]byte, 8+8+len(instanceIDBytes))
		binary.BigEndian.PutUint16(data[6:], cmdStreamRecover)
		binary.BigEndian.PutUint64(data[8:], msg.FromIndex)
		copy(data[16:], instanceIDBytes)
	case *StreamStopMessage:
		data = make([]byte, 8)
		binary.BigEndian.PutUint16(data[6:], cmdStreamStop)
	case *StreamStartedMessage:
		instanceIDBytes := []byte(msg.InstanceID)
		data = make([]byte, 8+len(instanceIDBytes))
		binary.BigEndian.PutUint16(data[6:], cmdStreamStarted)
		copy(data[8:], instanceIDBytes)
	case *PushStreamAddFilterMessage:
		data = make([]byte, 8+2)
		binary.BigEndian.PutUint16(data[6:], cmdPushStreamAddFilter)
		binary.BigEndian.PutUint16(data[8:], msg.FilterID)
	case *PushStreamRemoveFilterMessage:
		data = make([]byte, 8+2)
		binary.BigEndian.PutUint16(data[6:], cmdPushStreamRemoveFilter)
		binary.BigEndian.PutUint16(data[8:], msg.FilterID)
	case *PushStreamItemMessage:
		data = make([]byte, 8+8+len(msg.Data))
		binary.BigEndian.PutUint16(data[6:], cmdPushStreamItem)
		binary.BigEndian.PutUint64(data[8:], msg.EventIndex)
		copy(data[16:], msg.Data)
	case *PushStreamAdvanceMessage:
		data = make([]byte, 8+8)
		binary.BigEndian.PutUint16(data[6:], cmdPushStreamAdvance)
		binary.BigEndian.PutUint64(data[8:], msg.EventIndex)
	case *PushStreamEndMessage:
		msgBytes := []byte(msg.Message)
		data = make([]byte, 8+4+len(msgBytes))
		binary.BigEndian.PutUint16(data[6:], cmdPushStreamEnd)
		binary.BigEndian.PutUint32(data[8:], msg.Reason)
		copy(data[12:], msgBytes)

	default:
		return ErrUnknownMessage
	}

	// Every message puts this
	binary.BigEndian.PutUint32(data[0:], uint32(len(data)))
	binary.BigEndian.PutUint16(data[4:], channelID)

	log.Printf("writing %+v", data)
	_, err := wrt.writer.Write(data)
	return err
}
