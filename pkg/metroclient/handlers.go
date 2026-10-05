package metroclient

import (
	"time"

	"github.com/begulathemoai/metroclient/thirdparty/metroserver"
	"go.uber.org/zap"
)

func (c *Client) handleError(i *metroserver.ErrorPayload) {
	c.Logger.Info("Server threw error", zap.String("code", i.Code), zap.String("message", i.Message))
}

func (c *Client) handleJoinApproved(i *metroserver.JoinApprovedPayload) {

	c.UserID = i.UserID
	c.RoomState.PendingJoin = ""
	c.RoomState.updateFromMetroserverRoomState(i.State)
	c.RoomState.IsHost = false
	c.SessionToken.Store(i.SessionToken)
	c.Logger.Info("Joined room successfully", zap.String("room_code", i.RoomCode), zap.String("token", i.SessionToken), zap.String("user_id", i.UserID))
}

func (c *Client) handleJoinRejected(i *metroserver.JoinRejectedPayload) {
	c.Logger.Info("Join request was rejected", zap.String("RoomCode", c.RoomState.PendingJoin))
	c.RoomState.PendingJoin = ""
}

func (c *Client) handleServerCapabilities(i *metroserver.ServerCapabilitiesPayload) {
	c.codec.SetCompressionEnabled(i.SupportsCompression)
	c.Logger.Info("Got server capabilities", zap.Bool("supports_compression", i.SupportsCompression), zap.Bool("supports_protobuf", i.SupportsProtobuf), zap.String("server_version", i.ServerVersion))
}

func (c *Client) handlePlaybackSync(i *metroserver.PlaybackActionPayload) {
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

func (c *Client) handleUserJoined(i *metroserver.UserJoinedPayload) {
	// educated guesses : user just joined so they must not be the host and must be connected
	c.RoomState.Users = append(c.RoomState.Users, metroserver.UserInfo{UserID: i.UserID, Username: i.Username, IsHost: false, IsConnected: true})
}

func (c *Client) handleUserLeft(i *metroserver.UserLeftPayload) {
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

func (c *Client) handleRoomCreated(i *metroserver.RoomCreatedPayload) {
	c.RoomState.RoomCode = i.RoomCode
	c.RoomState.IsHost = true
	c.RoomState.Users = make([]metroserver.UserInfo, 1)
	c.RoomState.Users[0] = metroserver.UserInfo{UserID: i.UserID, Username: c.Username, IsHost: true, IsConnected: true}
	c.UserID = i.UserID
	c.SessionToken.Store(i.SessionToken)
	c.Logger.Info("Created room", zap.String("RoomCode", i.RoomCode))
	c.RequestSync()
}

func (c *Client) handleStateSync(i *metroserver.SyncStatePayload) {
	c.RoomState.CurrentTrack = i.CurrentTrack
	c.RoomState.IsPlaying = i.IsPlaying
	c.RoomState.Revision = i.Revision
	c.RoomState.LastUpdate = i.LastUpdate
	c.RoomState.Position = i.Position
	c.RoomState.Queue = i.Queue
	c.RoomState.Volume = i.Volume
}

func (c *Client) handleJoinRequest(i *metroserver.JoinRequestPayload) {
	c.Logger.Info("Received join request", zap.String("UserID", i.UserID), zap.String("Username", i.Username))
	c.RoomState.PendingJoinRequests[i.UserID] = i.Username
}
