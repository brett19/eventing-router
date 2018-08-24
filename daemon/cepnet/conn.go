package cepnet

import (
	"errors"
	"log"
	"net"
	"sync"
)

var ErrChannelClosed = errors.New("channel closed")
var ErrBadStream = errors.New("bad channel")
var ErrChannelExists = errors.New("channel exists")
var ErrSocketClosed = errors.New("socket is closed")

type Conn struct {
	lock     sync.Mutex
	conn     net.Conn
	reader   *Reader
	acceptCh chan *ConnChannel
	channels map[uint16]*ConnChannel
}

func wrapConn(rconn net.Conn) (*Conn, error) {
	conn := &Conn{
		conn:     rconn,
		reader:   NewReader(rconn),
		acceptCh: make(chan *ConnChannel),
		channels: make(map[uint16]*ConnChannel),
	}
	go conn.run()

	return conn, nil
}

func Connect(raddr string) (*Conn, error) {
	conn, err := net.Dial("tcp", raddr)
	if err != nil {
		return nil, err
	}

	return wrapConn(conn)
}

func (conn *Conn) writeMessage(channel *ConnChannel, msg Message) error {
	log.Printf("writing message %+v %+v", channel, msg)

	conn.lock.Lock()
	defer conn.lock.Unlock()

	log.Printf("acquired channel lock")

	channelID := uint16(0)
	if channel != nil {
		if conn.channels[channel.id] != channel {
			return ErrChannelClosed
		}

		channelID = channel.id
	}

	log.Printf("writing message channel %d", channelID)

	writer := NewWriter(conn.conn)
	return writer.WriteMessage(channelID, msg)
}

func (conn *Conn) closeStreamNoLock(channel *ConnChannel) error {
	ochannel, ok := conn.channels[channel.id]
	if !ok || ochannel != channel {
		return ErrBadStream
	}

	close(channel.msgQueue)
	delete(conn.channels, channel.id)

	return nil
}

func (conn *Conn) closeStream(channel *ConnChannel) error {
	conn.lock.Lock()
	defer conn.lock.Unlock()

	return conn.closeStreamNoLock(channel)
}

func (conn *Conn) writeCtrlMessage(msg Message) error {
	return conn.writeMessage(nil, msg)
}

func (conn *Conn) handleOpenChannel(msg *OpenChannelMessage) {
	conn.lock.Lock()
	defer conn.lock.Unlock()

	if _, ok := conn.channels[msg.ChannelID]; ok {
		conn.writeCtrlMessage(createErrMessage(ErrChannelExists))
		return
	}

	stream := &ConnChannel{
		id:       msg.ChannelID,
		parent:   conn,
		msgQueue: make(chan Message, 32),
	}

	conn.channels[msg.ChannelID] = stream

	conn.acceptCh <- stream
}

func (conn *Conn) handleCloseChannel(msg *CloseChannelMessage) {
	conn.lock.Lock()
	defer conn.lock.Unlock()

	stream, ok := conn.channels[msg.ChannelID]
	if !ok {
		conn.writeCtrlMessage(createErrMessage(ErrBadStream))
		return
	}

	err := conn.closeStreamNoLock(stream)
	if err != nil {
		conn.writeCtrlMessage(createErrMessage(err))
		return
	}
}

func (conn *Conn) handleCtrlMessage(msg Message) {
	switch msg := msg.(type) {
	case *OpenChannelMessage:
		conn.handleOpenChannel(msg)
	case *CloseChannelMessage:
		conn.handleCloseChannel(msg)
	default:
		// TODO: Write an error instead of ignoring bad control messages
		return
	}
}

func (conn *Conn) handleChannelMessage(streamID uint16, msg Message) {
	conn.lock.Lock()

	stream, ok := conn.channels[streamID]
	if !ok {
		// TODO: Write a message instead of ignoring the messages on invalid channels
		// ErrBadStream
		conn.lock.Unlock()
		return
	}

	conn.lock.Unlock()

	stream.msgQueue <- msg
}

func (conn *Conn) CreateChannel() (*ConnChannel, error) {
	return nil, nil
}

func (conn *Conn) AcceptChannel() (*ConnChannel, error) {
	stream, ok := <-conn.acceptCh
	if !ok {
		return nil, ErrSocketClosed
	}

	return stream, nil
}

func (conn *Conn) Close() error {
	conn.lock.Lock()
	defer conn.lock.Unlock()

	// Close the network connection
	conn.conn.Close()

	// Close the accept channel
	close(conn.acceptCh)

	// Close all the channels
	for _, stream := range conn.channels {
		close(stream.msgQueue)
	}
	conn.channels = make(map[uint16]*ConnChannel)

	return nil
}

func (conn *Conn) run() {
	for {
		channelID, msg, err := conn.reader.ReadMessage()
		if err != nil {
			log.Printf("failed to read: %s", err)
			conn.Close()
			return
		}

		if channelID == 0 {
			conn.handleCtrlMessage(msg)
			continue
		}

		conn.handleChannelMessage(channelID, msg)
	}
}
