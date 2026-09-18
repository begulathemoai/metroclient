package metroclient

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/begulathemoai/metroserverclient/metroserver"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	//"go.uber.org/zap"
)

const (
	PingInterval       = 30 * time.Second
	WriteTimeout       = 10 * time.Second
	ReadTimeout        = 10 * time.Second
	MaxReadMessageSize = 524288
)

type Client struct {
	SessionToken       atomic.Value
	RoomState          *metroserver.RoomState
	CurrentRoom        string
	UserID             string
	Conn               *websocket.Conn
	Send               chan []byte
	codec              *metroserver.MessageCodec
	Logger             *zap.Logger
	CompressionEnabled bool
}

func NewClient(url string, logger *zap.Logger) (c *Client, err error) {
	//header := http.Header{}
	//header.Set("Origin", "https://metroserverx.begulathemoai.dev")

	logger.Info("Connecting to remote server")
	connection, r, err := websocket.DefaultDialer.Dial(url, nil) //DialContext(context.Background(), url, header)
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
		Conn:   connection,
		Send:   make(chan []byte, 256),
		codec:  metroserver.NewMessageCodec(true),
		Logger: logger,
	}
	go c.writePump()
	go c.readPump()
	c.WriteMessage(&metroserver.ClientCapabilitiesPayload{
		SupportsProtobuf:    true,
		SupportsCompression: true,
		ClientVersion:       "1",
	})
	return c, nil
}

func (c *Client) WriteMessage(in any) (err error) {
	c.Logger.Debug("Sending message", zap.String("type", fmt.Sprintf("%T", in)), zap.Any("message", in))
	var T string
	switch in.(type) {
	case *metroserver.JoinRoomPayload:
		T = metroserver.MsgTypeJoinRoom
	case *metroserver.ClientCapabilitiesPayload:
		T = metroserver.MsgTypeClientCapabilities
	default:
		c.Logger.Error("couldn't send unhandled message type")
		return fmt.Errorf("couldn't send unhandled message type %T", in)
	}
	v, _ := c.codec.Encode(T, in)
	c.Send <- v

	return nil
}

func (c *Client) JoinRoom(code string) (err error) {
	c.Logger.Info("Attempting room join...")
	if len(code) != 8 {
		c.Logger.Error("room code format invalid (length)", zap.Int("code_length", len(code)))
		return fmt.Errorf("room code format invalid (length)")
	}
	for i := range code {
		if !strings.Contains("ABCDEFGHIJKLMNIOPQRSTUVWXYZ0123456789", string(code[i])) {
			c.Logger.Error("room code format invalid (chars)", zap.String("invalid_char", string(code[i])))
			return fmt.Errorf("room code format invalid (chars)")
		}
	}
	c.Logger.Debug("Room code format is good")
	c.WriteMessage(&metroserver.JoinRoomPayload{RoomCode: code, Username: "sexy_bot"})
	c.Logger.Debug("Join payload sent")
	return nil
}

// shamelessly stolen from metroserver by nyxiereal
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
				//logger.Debug("Failed to set write deadline", zap.String("client_id", c.clientID()), zap.Error(err))
				return
			}
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.Conn.WriteMessage(websocket.BinaryMessage, message); err != nil {
				//logger.Debug("Write error for client", zap.String("client_id", c.clientID()), zap.Error(err))
				return
			}

		case <-ticker.C:

			/*if err := c.Conn.SetWriteDeadline(time.Now().Add(WriteTimeout)); err != nil {
				//logger.Debug("Failed to set write deadline", zap.String("client_id", c.clientID()), zap.Error(err))
				return
			}
			if err := c.Conn.WriteMessage(websocket.PongMessage, nil); err != nil {
				return
			}*/
		}
	}
}

func (c *Client) readPump() {
	defer func() {
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(MaxReadMessageSize)
	/*if err := c.Conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
		c.Logger.Debug("Failed to set read deadline", zap.String("client_id", c.clientID()), zap.Error(err))
	}*/
	c.Conn.SetPingHandler(func(string) error {
		c.Logger.Debug("Received ping; now ponging")
		c.Conn.WriteControl(websocket.PongMessage, nil, time.Now().Add(WriteTimeout))
		if err := c.Conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
			//s.logger.Debug("Failed to set read deadline in pong handler", zap.String("client_id", c.clientID()), zap.Error(err))
		}
		return nil
	})
	/*c.Conn.SetPingHandler(func(string) error {
		/*if err := c.Conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
			//s.logger.Debug("Failed to set read deadline in pong handler", zap.String("client_id", c.clientID()), zap.Error(err))
		}
		return nil
	})*/

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				//s.logger.Debug("Read error for client", zap.String("client_id", c.clientID()), zap.Error(err))
			}
			break
		}

		/*if !c.allowMessage(time.Now()) {
			c.sendError(s.logger, "rate_limited", "Too many messages")
			continue
		}*/

		if err := c.Conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
			//s.logger.Debug("Failed to refresh read deadline", zap.String("client_id", c.clientID()), zap.Error(err))
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

	switch msgType {
	case metroserver.MsgTypeError:
		i := &metroserver.ErrorPayload{}
		err := metroserver.DecodePayload(payloadBytes, metroserver.MsgTypeError, i)
		if err != nil {
			fmt.Printf("error while decoding : %v", err)
			return
		}
		c.Logger.Info("Server threw error", zap.String("code", i.Code), zap.String("message", i.Message))
	case metroserver.MsgTypeJoinApproved:
		i := &metroserver.JoinApprovedPayload{}
		err := metroserver.DecodePayload(payloadBytes, metroserver.MsgTypeJoinApproved, i)
		if err != nil {
			c.Logger.Error("error while decoding", zap.Error(err))
			return
		}
		c.CurrentRoom = i.RoomCode
		c.UserID = i.UserID
		c.RoomState = i.State
		c.SessionToken.Store(i.SessionToken)
		c.Logger.Info("Joined room successfully", zap.String("room_code", i.RoomCode), zap.String("token", i.SessionToken), zap.String("user_id", i.UserID))
	case metroserver.MsgTypeServerCapabilities:
		i := &metroserver.ServerCapabilitiesPayload{}
		err := metroserver.DecodePayload(payloadBytes, metroserver.MsgTypeServerCapabilities, i)
		if err != nil {
			c.Logger.Error("error while decoding", zap.Error(err))
			return
		}
		c.codec.SetCompressionEnabled(i.SupportsCompression)
		c.Logger.Info("Got server capabilities", zap.Bool("supports_compression", i.SupportsCompression), zap.Bool("supports_protobuf", i.SupportsProtobuf), zap.String("server_version", i.ServerVersion))
	case metroserver.MsgTypeSyncPlayback:
		c.Logger.Debug("Received Sync Message")
	default:
		c.Logger.Info("Received message of unhandled type", zap.String("type", msgType))
	}
	//fmt.Printf("Received message of type %v containing\n%v\n", m.Type, string(m.Payload))

}

func (c *Client) Close() {
	c.Conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "goodbye......"), time.Now().Add(WriteTimeout))
}
