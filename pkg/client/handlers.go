package client

import (
	"fmt"
	"time"

	"github.com/begulathemoai/metroclient/thirdparty/metroserver"
	"go.uber.org/zap"
)

func (c *Client) handleError(payloadBytes []byte) {
	i := &metroserver.ErrorPayload{}
	err := metroserver.DecodePayload(payloadBytes, metroserver.MsgTypeError, i)
	if err != nil {
		fmt.Printf("error while decoding : %v", err)
		return
	}

	c.Logger.Info("Server threw error", zap.String("code", i.Code), zap.String("message", i.Message))
}

func (c *Client) handleJoinApproved(payloadBytes []byte) {
	i := &metroserver.JoinApprovedPayload{}
	err := metroserver.DecodePayload(payloadBytes, metroserver.MsgTypeJoinApproved, i)
	if err != nil {
		c.Logger.Error("error while decoding", zap.Error(err))
		return
	}
	c.UserID = i.UserID
	c.RoomState.PendingJoin = ""
	c.RoomState.UpdateFromMetroserverRoomState(i.State)
	c.RoomState.IsHost = false
	c.SessionToken.Store(i.SessionToken)
	c.Logger.Info("Joined room successfully", zap.String("room_code", i.RoomCode), zap.String("token", i.SessionToken), zap.String("user_id", i.UserID))
}

func (c *Client) handleJoinRejected(payloadBytes []byte) {
	i := &metroserver.JoinRejectedPayload{}
	err := metroserver.DecodePayload(payloadBytes, metroserver.MsgTypeJoinRejected, i)
	if err != nil {
		c.Logger.Error("error while decoding", zap.Error(err))
		return
	}
	c.Logger.Info("Join request was rejected", zap.String("RoomCode", c.RoomState.PendingJoin))
	c.RoomState.PendingJoin = ""
}

func (c *Client) handleServerCapabilities(payloadBytes []byte) {
	i := &metroserver.ServerCapabilitiesPayload{}
	err := metroserver.DecodePayload(payloadBytes, metroserver.MsgTypeServerCapabilities, i)
	if err != nil {
		c.Logger.Error("error while decoding", zap.Error(err))
		return
	}
	c.codec.SetCompressionEnabled(i.SupportsCompression)
	c.Logger.Info("Got server capabilities", zap.Bool("supports_compression", i.SupportsCompression), zap.Bool("supports_protobuf", i.SupportsProtobuf), zap.String("server_version", i.ServerVersion))
}

func (c *Client) handlePlaybackSync(payloadBytes []byte) {
	i := &metroserver.PlaybackActionPayload{}
	err := metroserver.DecodePayload(payloadBytes, metroserver.MsgTypeSyncPlayback, i)
	if err != nil {
		c.Logger.Error("error while decoding", zap.Error(err))
		return
	}
	c.RoomState.Revision = i.Revision
	c.RoomState.QueueTitle = i.QueueTitle
	c.RoomState.LastUpdate = time.Now().UnixMilli()
	switch i.Action {
	case metroserver.ActionSyncQueue:
		c.RoomState.CurrentTrack = i.TrackInfo
		c.RoomState.Position = i.Position
		c.Logger.Info("Received queue update")
	case metroserver.ActionPlay:
		if !c.RoomState.IsPlaying {
			c.Logger.Info("Resumed playback")
		}
		c.RoomState.Position = i.Position
		c.RoomState.IsPlaying = true

	case metroserver.ActionChangeTrack:
		c.RoomState.CurrentTrack = i.TrackInfo
		c.RoomState.Position = i.Position
		c.Logger.Info("Changed track", zap.String("new_track", i.TrackInfo.Title))
	case metroserver.ActionPause:
		c.Logger.Info("Paused playback")
		c.RoomState.IsPlaying = false
	default:
		c.Logger.Info("Received playback action of unhandled type", zap.String("type", i.Action))
	}
}

func (c *Client) handleUserLeft(payloadBytes []byte) {
	c.Logger.Debug("Received user_left")
	i := &metroserver.UserLeftPayload{}
	err := metroserver.DecodePayload(payloadBytes, metroserver.MsgTypeSyncPlayback, i)
	if err != nil {
		c.Logger.Error("error while decoding", zap.Error(err))
		return
	}
	idx := 0
	for id, u := range c.RoomState.Users {
		if u.UserID == i.UserID {
			idx = id
		}
	}
	// we remove the user that just left
	c.RoomState.Users[idx] = c.RoomState.Users[len(c.RoomState.Users)-1]
	c.RoomState.Users = c.RoomState.Users[:len(c.RoomState.Users)-1]
}

func (c *Client) handleRoomCreated(payloadBytes []byte) {
	c.Logger.Debug("Received room_created")
	i := &metroserver.RoomCreatedPayload{}
	err := metroserver.DecodePayload(payloadBytes, metroserver.MsgTypeRoomCreated, i)
	if err != nil {
		c.Logger.Error("error while decoding", zap.Error(err))
		return
	}
	c.RoomState.RoomCode = i.RoomCode
	c.RoomState.IsHost = true
	c.UserID = i.UserID
	c.SessionToken.Store(i.SessionToken)
	c.Logger.Info("Created room", zap.String("RoomCode", i.RoomCode))
	c.RequestSync()
}

func (c *Client) handleStateSync(payloadBytes []byte) {
	i := &metroserver.SyncStatePayload{}
	err := metroserver.DecodePayload(payloadBytes, metroserver.MsgTypeSyncState, i)
	if err != nil {
		c.Logger.Error("error while decoding", zap.Error(err))
		return
	}
	c.RoomState.CurrentTrack = i.CurrentTrack
	c.RoomState.IsPlaying = i.IsPlaying
	c.RoomState.Revision = i.Revision
	c.RoomState.LastUpdate = i.LastUpdate
	c.RoomState.Position = i.Position
	c.RoomState.Queue = i.Queue
	c.RoomState.Volume = i.Volume
}

func (c *Client) handleJoinRequest(payloadBytes []byte) {
	i := &metroserver.JoinRequestPayload{}
	err := metroserver.DecodePayload(payloadBytes, metroserver.MsgTypeJoinRequest, i)
	if err != nil {
		c.Logger.Error("error while decoding", zap.Error(err))
		return
	}
	c.Logger.Info("Received join request", zap.String("UserID", i.UserID), zap.String("Username", i.Username))
	c.RoomState.PendingJoinRequests[i.UserID] = i.Username
}
