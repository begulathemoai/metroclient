package metroclient

import (
	"fmt"

	"github.com/begulathemoai/metroclient/thirdparty/metroserver"
	"go.uber.org/zap"
)

type RoomState struct {
	PendingJoin         string
	RoomCode            string
	HostID              string
	Users               []metroserver.UserInfo
	Queue               []metroserver.TrackInfo
	QueueTitle          string
	CurrentTrack        *metroserver.TrackInfo
	IsPlaying           bool
	IsHost              bool
	Position            int64
	LastUpdate          int64
	Volume              float64
	Revision            uint64
	PendingJoinRequests map[string]string
}

func (r *RoomState) clear() {
	r.PendingJoin = ""
	r.RoomCode = ""
	r.HostID = ""
	r.Users = make([]metroserver.UserInfo, 0)
	r.Queue = make([]metroserver.TrackInfo, 0)
	r.QueueTitle = ""
	r.CurrentTrack = nil
	r.IsPlaying = false
	r.IsHost = false
	r.Position = 0
	r.LastUpdate = 0
	r.Volume = 0
	r.Revision = 0
	r.PendingJoinRequests = make(map[string]string)

}

func (r *RoomState) updateFromMetroserverRoomState(msrs *metroserver.RoomState) {
	r.RoomCode = msrs.RoomCode
	r.HostID = msrs.HostID
	r.Users = msrs.Users
	r.Queue = msrs.Queue
	r.CurrentTrack = msrs.CurrentTrack
	r.IsPlaying = msrs.IsPlaying
	r.Position = msrs.Position
	r.LastUpdate = msrs.LastUpdate
	r.Volume = msrs.Volume
	r.Revision = msrs.Revision
}

func (r *RoomState) toMetroserverRoomState() (msrs *metroserver.RoomState) {
	msrs = &metroserver.RoomState{
		RoomCode:     r.RoomCode,
		HostID:       r.HostID,
		Users:        r.Users,
		Queue:        r.Queue,
		CurrentTrack: r.CurrentTrack,
		IsPlaying:    r.IsPlaying,
		Position:     r.Position,
		LastUpdate:   r.LastUpdate,
		Volume:       r.Volume,
		Revision:     r.Revision,
	}
	return
}

func (c *Client) AcceptJoinRequest(UserID string) (err error) {
	_, ok := c.RoomState.PendingJoinRequests[UserID]
	if !ok {
		return fmt.Errorf("when accepting join request : no user with this id has requested to join")
	}

	c.Logger.Info("Accepting join request", zap.String("UserID", UserID), zap.String("Username", c.RoomState.PendingJoinRequests[UserID]))
	err = c.WriteMessage(metroserver.MsgTypeApproveJoin, &metroserver.ApproveJoinPayload{UserID: UserID})
	if err != nil {
		return fmt.Errorf("when accepting join request : %w", err)
	}
	delete(c.RoomState.PendingJoinRequests, UserID)
	return nil
}

func (c *Client) JoinRoom(code string) (err error) {
	if c.RoomState.RoomCode != "" || c.RoomState.PendingJoin != "" {
		return fmt.Errorf("when joining room : cannot join room if already in one / if request was already sent (there is currently no way for clients to cancel a join request)")
	}
	c.RoomState.PendingJoin = code
	c.Logger.Info("Attempting room join...")
	c.WriteMessage(metroserver.MsgTypeJoinRoom, &metroserver.JoinRoomPayload{RoomCode: code, Username: c.Username})
	return nil
}

func (c *Client) CreateRoom() (err error) {
	c.Logger.Info("Attempting room creation...")
	c.WriteMessage(metroserver.MsgTypeCreateRoom, &metroserver.CreateRoomPayload{
		Username: c.Username,
	})
	c.Logger.Info("Room create payload sent")
	return nil
}

func (c *Client) LeaveRoom() (err error) {
	c.Logger.Info("Attempting to leave room")
	/*if c.RoomState.RoomCode == "" {
		return fmt.Errorf("when leaving room : this client isn't in any room")
	}*/

	c.WriteMessage(metroserver.MsgTypeLeaveRoom, nil)
	c.RoomState.clear()
	c.UserID = ""
	c.SessionToken.Store("")
	c.Logger.Info("Left room")
	return nil
}

func (c *Client) RequestSync() (err error) {
	c.Logger.Debug("Requesting Sync")
	c.WriteMessage(metroserver.MsgTypeRequestSync, nil)

	return err
}
