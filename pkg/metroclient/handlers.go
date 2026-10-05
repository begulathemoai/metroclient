package metroclient

import (
	"slices"
	"time"

	"github.com/begulathemoai/metroclient/thirdparty/metroserver"
	"go.uber.org/zap"
)

func (c *Client) handleError(i *metroserver.ErrorPayload) {
	c.Logger.Info("Server threw error", zap.String("code", i.Code), zap.String("message", i.Message))
}

func (c *Client) handleJoinApproved(i *metroserver.JoinApprovedPayload) {
	c.RoomState.mu.Lock()
	defer c.RoomState.mu.Unlock()

	c.UserID = i.UserID
	c.RoomState.PendingJoin = ""
	c.RoomState.updateFromMetroserverRoomState(i.State)
	c.RoomState.IsHost = false
	c.SessionToken.Store(i.SessionToken)
	c.Logger.Info("Joined room successfully", zap.String("room_code", i.RoomCode), zap.String("token", i.SessionToken), zap.String("user_id", i.UserID))
}

func (c *Client) handleJoinRejected(i *metroserver.JoinRejectedPayload) {
	c.RoomState.mu.Lock()
	defer c.RoomState.mu.Unlock()
	c.Logger.Info("Join request was rejected", zap.String("RoomCode", c.RoomState.PendingJoin), zap.String("Reason", i.Reason))
	c.RoomState.PendingJoin = ""
}

func (c *Client) handleServerCapabilities(i *metroserver.ServerCapabilitiesPayload) {
	c.RoomState.mu.Lock()
	defer c.RoomState.mu.Unlock()
	c.codec.SetCompressionEnabled(i.SupportsCompression)
	c.ServInfo = &ServerInfo{SupportsProtobuf: i.SupportsProtobuf, SupportsCompression: i.SupportsCompression, ServerVersion: i.ServerVersion}
	c.Logger.Info("Got server capabilities", zap.Bool("supports_compression", i.SupportsCompression), zap.Bool("supports_protobuf", i.SupportsProtobuf), zap.String("server_version", i.ServerVersion))
}

func (c *Client) handlePlaybackSync(i *metroserver.PlaybackActionPayload) {
	c.RoomState.mu.Lock()
	defer c.RoomState.mu.Unlock()
	c.RoomState.Revision = i.Revision
	if i.QueueTitle != "" {
		c.RoomState.QueueTitle = i.QueueTitle
	}
	c.RoomState.LastUpdate = time.Now().UnixMilli()
	switch i.Action {
	case metroserver.ActionQueueAdd:
		if i.InsertNext {
			curr := slices.Index(c.RoomState.Queue, *c.RoomState.CurrentTrack)
			if curr != -1 {
				c.RoomState.Queue = slices.Insert(c.RoomState.Queue, curr+1, *i.TrackInfo)
			}

		} else {
			c.RoomState.Queue = append(c.RoomState.Queue, *i.TrackInfo)
		}
		c.Logger.Info("Added song to queue")
	case metroserver.ActionQueueRemove:
		idx := slices.Index(c.RoomState.Queue, *i.TrackInfo)
		if idx != -1 {
			c.RoomState.Queue = slices.Delete(c.RoomState.Queue, idx, idx)
		}
		c.Logger.Info("Removed track from queue")
	case metroserver.ActionQueueClear:
		c.RoomState.Queue = make([]metroserver.TrackInfo, 0)
		c.Logger.Info("Cleared queue")
	case metroserver.ActionSeek:
		c.RoomState.Position = i.Position + (time.Now().UnixMilli() - i.ServerTime)
		c.Logger.Info("Seeked")
	case metroserver.ActionSkipNext:
		idx := slices.Index(c.RoomState.Queue, *c.RoomState.CurrentTrack)
		if len(c.RoomState.Queue) > idx+1 {

			c.RoomState.CurrentTrack = &c.RoomState.Queue[idx+1]
		}
		c.Logger.Info("Skipped to next track")
	case metroserver.ActionSkipPrev:
		idx := slices.Index(c.RoomState.Queue, *c.RoomState.CurrentTrack)
		if idx-1 >= 0 {

			c.RoomState.CurrentTrack = &c.RoomState.Queue[idx-1]
		}
		c.Logger.Info("Skipped to previous track")
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
	case metroserver.ActionSetVolume:
		c.RoomState.Volume = i.Volume

	default:
		c.Logger.Info("Received playback action of unhandled type", zap.String("type", i.Action))
	}
}

func (c *Client) handleUserJoined(i *metroserver.UserJoinedPayload) {
	c.RoomState.mu.Lock()
	defer c.RoomState.mu.Unlock()
	// educated guesses : user just joined so they must not be the host and must be connected
	c.RoomState.Users = append(c.RoomState.Users, metroserver.UserInfo{UserID: i.UserID, Username: i.Username, IsHost: false, IsConnected: true})
}

func (c *Client) handleUserLeft(i *metroserver.UserLeftPayload) {
	c.RoomState.mu.Lock()
	defer c.RoomState.mu.Unlock()
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
	c.RoomState.mu.Lock()
	defer c.RoomState.mu.Unlock()
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
	c.RoomState.mu.Lock()
	defer c.RoomState.mu.Unlock()
	c.RoomState.CurrentTrack = i.CurrentTrack
	c.RoomState.IsPlaying = i.IsPlaying
	c.RoomState.Revision = i.Revision
	c.RoomState.LastUpdate = i.LastUpdate
	c.RoomState.Position = i.Position
	c.RoomState.Queue = i.Queue
	c.RoomState.Volume = i.Volume
}

func (c *Client) handleJoinRequest(i *metroserver.JoinRequestPayload) {
	c.RoomState.mu.Lock()
	defer c.RoomState.mu.Unlock()
	c.Logger.Info("Received join request", zap.String("UserID", i.UserID), zap.String("Username", i.Username))
	c.RoomState.PendingJoinRequests[i.UserID] = i.Username
}

func (c *Client) handleKicked(i *metroserver.KickedPayload) {
	c.RoomState.mu.Lock()
	defer c.RoomState.mu.Unlock()
	c.Logger.Info("Got kicked from room", zap.String("Reason", i.Reason))
	c.RoomState.clear()
	c.UserID = ""
	c.SessionToken.Store("")
}

func (c *Client) handleHostChange(i *metroserver.HostChangedPayload) {
	c.RoomState.mu.Lock()
	defer c.RoomState.mu.Unlock()
	c.Logger.Info("Host changed", zap.String("NewID", i.NewHostID), zap.String("NewUsername", i.NewHostName))
	c.RoomState.HostID = i.NewHostID
	if c.UserID == i.NewHostID {
		c.RoomState.IsHost = true
	} else {
		c.RoomState.IsHost = false
	}
	for idx := range c.RoomState.Users {
		if c.RoomState.Users[idx].UserID == i.NewHostID {
			c.RoomState.Users[idx].IsHost = true
		} else {
			c.RoomState.Users[idx].IsHost = false
		}
	}
}
