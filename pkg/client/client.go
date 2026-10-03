package client

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/begulathemoai/metroclient/thirdparty/metroserver"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const (
	PingInterval       = 30 * time.Second
	WriteTimeout       = 10 * time.Second
	ReadTimeout        = 40 * time.Second
	MaxReadMessageSize = 524288
)

type Client struct {
	Username            string
	SessionToken        atomic.Value
	RoomState           *RoomState
	UserID              string
	Conn                *websocket.Conn
	Send                chan []byte
	codec               *metroserver.MessageCodec
	Logger              *zap.Logger
	CompressionEnabled  bool
	awaits              map[chan int]string
	notificationHandler func(string, []byte)
}

func NewClient(url string, logger *zap.Logger) (c *Client, err error) {

	logger.Info("Connecting to remote server")
	h := http.Header{}
	h.Add("User-Agent", "dev.begulathemoai.metroclient")
	connection, r, err := websocket.DefaultDialer.Dial(url, h)
	if err != nil {
		logger.Error("Connection failed")
		return nil, fmt.Errorf("when dialing url : %w", err)
	}

	if r.StatusCode != 101 {
		logger.Error("Unexpected status code", zap.Int("StatusCode", r.StatusCode))
		return nil, fmt.Errorf("got status code : %v", r.Status)
	}
	logger.Info("Connected with status code ", zap.Int("StatusCode", r.StatusCode))

	c = &Client{
		Username: "sexy_bot",
		Conn:     connection,
		Send:     make(chan []byte, 256),
		codec:    metroserver.NewMessageCodec(true),
		Logger:   logger,
	}
	c.RoomState = &RoomState{Users: make([]metroserver.UserInfo, 0), Queue: make([]metroserver.TrackInfo, 0), PendingJoinRequests: make(map[string]string)}
	go c.writePump()
	go c.readPump()
	c.WriteMessage(&metroserver.ClientCapabilitiesPayload{
		SupportsProtobuf:    true,
		SupportsCompression: true,
		ClientVersion:       "1",
	})
	return c, nil
}

func (c *Client) SetNotificationHandler(h func(string, []byte)) {
	c.notificationHandler = h
}

func (c *Client) WriteMessage(in any) (err error) {
	c.Logger.Debug("Sending message", zap.String("type", fmt.Sprintf("%T", in)), zap.Any("message", in))
	var T string
	switch in.(type) {
	case *metroserver.JoinRoomPayload:
		T = metroserver.MsgTypeJoinRoom
	case *metroserver.ClientCapabilitiesPayload:
		T = metroserver.MsgTypeClientCapabilities
	case *metroserver.LeaveRoomPayload:
		T = metroserver.MsgTypeLeaveRoom
	case *metroserver.CreateRoomPayload:
		T = metroserver.MsgTypeCreateRoom
	case *metroserver.ApproveJoinPayload:
		T = metroserver.MsgTypeApproveJoin
	default:
		c.Logger.Error("couldn't send unhandled message type")
		return fmt.Errorf("couldn't send unhandled message type %T", in)
	}
	v, _ := c.codec.Encode(T, in)
	c.Send <- v

	return nil
}

// shamelessly stolen and adapted from github.com/MetrolistGroup/metroserver by nyxiereal
func (c *Client) writePump() {
	ticker := time.NewTicker(PingInterval)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			if err := c.Conn.SetWriteDeadline(time.Now().Add(WriteTimeout)); err != nil {
				c.Logger.Debug("Failed to set write deadline", zap.Error(err))
				return
			}
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.Conn.WriteMessage(websocket.BinaryMessage, message); err != nil {
				c.Logger.Debug("Write error for client", zap.Error(err))
				return
			}

		case <-ticker.C:

			if err := c.Conn.SetWriteDeadline(time.Now().Add(WriteTimeout)); err != nil {
				c.Logger.Debug("Failed to set write deadline", zap.Error(err))
				return
			}
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.Logger.Info("Write pump broken", zap.Error(err))
				return
			}
		}
	}
}

// shamelessly stolen and adapted from github.com/MetrolistGroup/metroserver by nyxiereal
func (c *Client) readPump() {
	defer func() {
		c.Close()
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(MaxReadMessageSize)
	if err := c.Conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
		c.Logger.Debug("Failed to set read deadline", zap.Error(err))
	}
	c.Conn.SetPingHandler(func(string) error {
		c.Logger.Debug("Received ping; now ponging")
		c.Conn.WriteControl(websocket.PongMessage, nil, time.Now().Add(WriteTimeout))
		if err := c.Conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
			c.Logger.Debug("Failed to set read deadline in ping handler", zap.Error(err))
		}
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.Logger.Debug("Read error", zap.Error(err))
			}
			c.Logger.Info("Read pump broken", zap.Error(err))
			break
		}

		if err := c.Conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
			c.Logger.Debug("Failed to refresh read deadline", zap.Error(err))
			break
		}
		c.handleMessage(message)
	}
}

func (c *Client) handleMessage(message []byte) {
	//t, d, e := c.codec.Decode(message)
	msgType, payloadBytes, err := c.codec.Decode(message)
	if err != nil {
		c.Logger.Info("Received invalid message")
		return
	}

	if msgType == "" {
		c.Logger.Info("Received no message type")
		return
	}
	for ch, t := range c.awaits {
		if t == msgType {
			ch <- 1
		}
	}
	if c.notificationHandler != nil {
		c.notificationHandler(msgType, payloadBytes)
	}
	switch msgType {
	// the server sent us an error
	case metroserver.MsgTypeError:
		c.handleError(payloadBytes)
	// our join request was approved by the server
	case metroserver.MsgTypeJoinApproved:
		c.handleJoinApproved(payloadBytes)

	// server capabilities (protobuf, compression)
	case metroserver.MsgTypeServerCapabilities:
		c.handleServerCapabilities(payloadBytes)
	// playback event/action
	case metroserver.MsgTypeSyncPlayback:
		c.handlePlaybackSync(payloadBytes)
	// a user left the room
	case metroserver.MsgTypeUserLeft:
		c.handleUserLeft(payloadBytes)
	// the server created us a room
	case metroserver.MsgTypeRoomCreated:
		c.handleRoomCreated(payloadBytes)
	// the server sent us a snapshot of the room state
	case metroserver.MsgTypeSyncState:
		c.handleStateSync(payloadBytes)
	// someone sent us a join request
	case metroserver.MsgTypeJoinRequest:
		c.handleJoinRequest(payloadBytes)
	case metroserver.MsgTypeJoinRejected:
		c.handleJoinRejected(payloadBytes)
	default:
		c.Logger.Info("Received message of unhandled type", zap.String("type", msgType))
	}

}

// returns true if the message was received first, false if the timeout occured first
func (c *Client) AwaitMessageOrTimeout(msg string, timeout time.Duration) bool {
	nch := make(chan int)
	tch := time.After(timeout)

	select {
	case <-nch:
		return true
	case <-tch:
		close(nch)
		return false
	}
}

func (c *Client) Close() {
	if c.RoomState.RoomCode != "" {
		c.LeaveRoom()
		c.AwaitMessageOrTimeout(metroserver.MsgTypeUserLeft, time.Second)
	}
	c.Logger.Info("Closing connection...")
	defer c.Conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "goodbye......"), time.Now().Add(WriteTimeout))
}
