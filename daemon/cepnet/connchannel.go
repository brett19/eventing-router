package cepnet

type ConnChannel struct {
	id       uint16
	parent   *Conn
	msgQueue chan Message
}

func (channel *ConnChannel) Conn() *Conn {
	return channel.parent
}

func (channel *ConnChannel) ReadMessage() (Message, error) {
	if msg, ok := <-channel.msgQueue; ok {
		return msg, nil
	}
	return nil, ErrChannelClosed
}

func (channel *ConnChannel) WriteMessage(msg Message) error {
	return channel.parent.writeMessage(channel, msg)
}

func (channel *ConnChannel) Close() error {
	return channel.parent.closeStream(channel)
}
