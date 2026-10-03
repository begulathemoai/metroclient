package metroserver

import (
	"fmt"

	pb "github.com/begulathemoai/metroclient/proto"
	"google.golang.org/protobuf/proto"
)

func fromProtoUser(user *pb.UserInfo) (out UserInfo) {
	return UserInfo{UserID: user.UserId, Username: user.Username, IsHost: user.IsHost, IsConnected: user.IsConnected}
}

func fromProtoTrack(track *pb.TrackInfo) (out *TrackInfo) {
	return &TrackInfo{ID: track.Id, Title: track.Title, Artist: track.Artist, Album: track.Album, Duration: track.Duration, Thumbnail: track.Thumbnail, SuggestedBy: track.SuggestedBy}
}

func fromProtoState(state any) (out any) {
	switch state := state.(type) {
	case *pb.RoomState:
		cstate := state
		var users []UserInfo = make([]UserInfo, len(cstate.Users))
		for i := range cstate.Users {
			new_user := fromProtoUser(cstate.Users[i])
			users = append(users, new_user)
		}
		track := fromProtoTrack(cstate.CurrentTrack)
		var queue []TrackInfo = make([]TrackInfo, len(cstate.Queue))
		for i := range cstate.Queue {
			new_track := *fromProtoTrack(cstate.Queue[i])
			queue = append(queue, new_track)
		}
		return &RoomState{RoomCode: cstate.RoomCode, HostID: cstate.HostId, Users: users, CurrentTrack: track, IsPlaying: cstate.IsPlaying, Position: cstate.Position, LastUpdate: cstate.LastUpdate, Volume: float64(cstate.Volume), Queue: queue, Revision: cstate.Revision}
	default:
		return nil
	}

}

// fromProtoMessage converts protobuf messages to Go structs
func fromProtoMessage(msgType string, data []byte) (any, error) {
	switch msgType {
	case MsgTypeError:
		var pbb pb.ErrorPayload
		if err := proto.Unmarshal(data, &pbb); err != nil {
			return nil, err
		}
		return &ErrorPayload{Code: pbb.Code, Message: pbb.Message}, nil
	case MsgTypeJoinApproved:
		var pbb pb.JoinApprovedPayload
		if err := proto.Unmarshal(data, &pbb); err != nil {
			return nil, err
		}

		return &JoinApprovedPayload{RoomCode: pbb.RoomCode, UserID: pbb.UserId, SessionToken: pbb.SessionToken, State: fromProtoState(pbb.State).(*RoomState)}, nil
	case MsgTypeServerCapabilities:
		var pbb pb.ServerCapabilities
		if err := proto.Unmarshal(data, &pbb); err != nil {
			return nil, err
		}

		return &ServerCapabilitiesPayload{SupportsProtobuf: pbb.SupportsProtobuf, SupportsCompression: pbb.SupportsCompression, ServerVersion: pbb.ServerVersion}, nil
	case MsgTypeCreateRoom:
		var pbb pb.CreateRoomPayload
		if err := proto.Unmarshal(data, &pbb); err != nil {
			return nil, err
		}
		return &CreateRoomPayload{Username: pbb.Username}, nil
	case MsgTypeJoinRoom:
		var pbb pb.JoinRoomPayload
		if err := proto.Unmarshal(data, &pbb); err != nil {
			return nil, err
		}
		return &JoinRoomPayload{RoomCode: pbb.RoomCode, Username: pbb.Username}, nil
	case MsgTypeApproveJoin:
		var pbb pb.ApproveJoinPayload
		if err := proto.Unmarshal(data, &pbb); err != nil {
			return nil, err
		}
		return &ApproveJoinPayload{UserID: pbb.UserId}, nil
	case MsgTypeRejectJoin:
		var pbb pb.RejectJoinPayload
		if err := proto.Unmarshal(data, &pbb); err != nil {
			return nil, err
		}
		return &RejectJoinPayload{UserID: pbb.UserId, Reason: pbb.Reason}, nil
	case MsgTypeSyncPlayback:
		if err := validatePlaybackActionCardinality(data); err != nil {
			return nil, err
		}
		var pbMsg pb.PlaybackActionPayload
		if err := proto.Unmarshal(data, &pbMsg); err != nil {
			return nil, err
		}
		payload := &PlaybackActionPayload{
			Action:               pbMsg.Action,
			TrackID:              pbMsg.TrackId,
			Position:             pbMsg.Position,
			InsertNext:           pbMsg.InsertNext,
			QueueTitle:           pbMsg.QueueTitle,
			Volume:               float64(pbMsg.Volume),
			ServerTime:           pbMsg.ServerTime,
			Revision:             pbMsg.Revision,
			CapturedAtServerTime: pbMsg.CapturedAtServerTime,
		}
		if pbMsg.TrackInfo != nil {
			payload.TrackInfo = protoToTrackInfo(pbMsg.TrackInfo)
		}
		if pbMsg.Queue != nil {
			if len(pbMsg.Queue) > MaxQueueInputSize {
				return nil, fmt.Errorf("queue has more than %d entries", MaxQueueInputSize)
			}
			payload.Queue = make([]TrackInfo, len(pbMsg.Queue))
			for i, track := range pbMsg.Queue {
				payload.Queue[i] = *protoToTrackInfo(track)
			}
		}
		return payload, nil
	case MsgTypePlaybackAction:
		if err := validatePlaybackActionCardinality(data); err != nil {
			return nil, err
		}
		var pbMsg pb.PlaybackActionPayload
		if err := proto.Unmarshal(data, &pbMsg); err != nil {
			return nil, err
		}
		payload := &PlaybackActionPayload{
			Action:               pbMsg.Action,
			TrackID:              pbMsg.TrackId,
			Position:             pbMsg.Position,
			InsertNext:           pbMsg.InsertNext,
			QueueTitle:           pbMsg.QueueTitle,
			Volume:               float64(pbMsg.Volume),
			ServerTime:           pbMsg.ServerTime,
			Revision:             pbMsg.Revision,
			CapturedAtServerTime: pbMsg.CapturedAtServerTime,
		}
		if pbMsg.TrackInfo != nil {
			payload.TrackInfo = protoToTrackInfo(pbMsg.TrackInfo)
		}
		if pbMsg.Queue != nil {
			if len(pbMsg.Queue) > MaxQueueInputSize {
				return nil, fmt.Errorf("queue has more than %d entries", MaxQueueInputSize)
			}
			payload.Queue = make([]TrackInfo, len(pbMsg.Queue))
			for i, track := range pbMsg.Queue {
				payload.Queue[i] = *protoToTrackInfo(track)
			}
		}
		return payload, nil
	case MsgTypeBufferReady:
		var pb pb.BufferReadyPayload
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return &BufferReadyPayload{TrackID: pb.TrackId}, nil
	case MsgTypePing:
		var pbPayload pb.PingPayload
		if err := proto.Unmarshal(data, &pbPayload); err != nil {
			return nil, err
		}
		return &PingPayload{ClientTime: pbPayload.ClientTime, Sequence: pbPayload.Sequence}, nil
	case MsgTypeKickUser:
		var pb pb.KickUserPayload
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return &KickUserPayload{UserID: pb.UserId, Reason: pb.Reason}, nil
	case MsgTypeTransferHost:
		var pb pb.TransferHostPayload
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return &TransferHostPayload{NewHostID: pb.NewHostId}, nil
	case MsgTypeSuggestTrack:
		var pbMsg pb.SuggestTrackPayload
		if err := proto.Unmarshal(data, &pbMsg); err != nil {
			return nil, err
		}
		payload := &SuggestTrackPayload{}
		if pbMsg.TrackInfo != nil {
			payload.TrackInfo = protoToTrackInfo(pbMsg.TrackInfo)
		}
		return payload, nil
	case MsgTypeApproveSuggestion:
		var pb pb.ApproveSuggestionPayload
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return &ApproveSuggestionPayload{SuggestionID: pb.SuggestionId}, nil
	case MsgTypeRejectSuggestion:
		var pb pb.RejectSuggestionPayload
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return &RejectSuggestionPayload{SuggestionID: pb.SuggestionId, Reason: pb.Reason}, nil
	case MsgTypeReconnect:
		var pb pb.ReconnectPayload
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return &ReconnectPayload{SessionToken: pb.SessionToken}, nil
	case MsgTypeClientCapabilities:
		var pb pb.ClientCapabilities
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return &ClientCapabilitiesPayload{SupportsProtobuf: pb.SupportsProtobuf, SupportsCompression: pb.SupportsCompression, ClientVersion: pb.ClientVersion}, nil
	default:
		return nil, fmt.Errorf("unsupported message type: %s", msgType)
	}
}

// decodePayload decodes a protobuf payload into the target interface
func DecodePayload(payloadBytes []byte, msgType string, target interface{}) error {
	// Use fromProtoMessage to convert protobuf to Go struct
	payload, err := fromProtoMessage(msgType, payloadBytes)
	if err != nil {
		return err
	}
	// Copy the decoded payload to target using safe type assertion
	targetVal := target
	switch t := targetVal.(type) {
	case *CreateRoomPayload:
		p, ok := payload.(*CreateRoomPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected CreateRoomPayload, got %T", payload)
		}
		*t = *p
	case *JoinRoomPayload:
		p, ok := payload.(*JoinRoomPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected JoinRoomPayload, got %T", payload)
		}
		*t = *p
	case *ApproveJoinPayload:
		p, ok := payload.(*ApproveJoinPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected ApproveJoinPayload, got %T", payload)
		}
		*t = *p
	case *RejectJoinPayload:
		p, ok := payload.(*RejectJoinPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected RejectJoinPayload, got %T", payload)
		}
		*t = *p
	case *PlaybackActionPayload:
		p, ok := payload.(*PlaybackActionPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected PlaybackActionPayload, got %T", payload)
		}
		*t = *p
	case *BufferReadyPayload:
		p, ok := payload.(*BufferReadyPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected BufferReadyPayload, got %T", payload)
		}
		*t = *p
	case *PingPayload:
		p, ok := payload.(*PingPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected PingPayload, got %T", payload)
		}
		*t = *p
	case *KickUserPayload:
		p, ok := payload.(*KickUserPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected KickUserPayload, got %T", payload)
		}
		*t = *p
	case *SuggestTrackPayload:
		p, ok := payload.(*SuggestTrackPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected SuggestTrackPayload, got %T", payload)
		}
		*t = *p
	case *ApproveSuggestionPayload:
		p, ok := payload.(*ApproveSuggestionPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected ApproveSuggestionPayload, got %T", payload)
		}
		*t = *p
	case *RejectSuggestionPayload:
		p, ok := payload.(*RejectSuggestionPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected RejectSuggestionPayload, got %T", payload)
		}
		*t = *p
	case *ReconnectPayload:
		p, ok := payload.(*ReconnectPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected ReconnectPayload, got %T", payload)
		}
		*t = *p
	case *TransferHostPayload:
		p, ok := payload.(*TransferHostPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected TransferHostPayload, got %T", payload)
		}
		*t = *p
	case *ClientCapabilitiesPayload:
		p, ok := payload.(*ClientCapabilitiesPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected ClientCapabilitiesPayload, got %T", payload)
		}
		*t = *p
	case *ErrorPayload:
		p, ok := payload.(*ErrorPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected ErrorPayload, got %T", payload)
		}
		*t = *p
	case *JoinApprovedPayload:
		p, ok := payload.(*JoinApprovedPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected JoinApprovedPayload, got %T", payload)
		}
		*t = *p
	case *ServerCapabilitiesPayload:
		p, ok := payload.(*ServerCapabilitiesPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected ServerCapabilitiesPayload, got %T", payload)
		}
		*t = *p
	case *UserLeftPayload:
		p, ok := payload.(*UserLeftPayload)
		if !ok {
			return fmt.Errorf("payload type mismatch: expected UserLeftPayload, got %T", payload)
		}
		*t = *p
	default:
		return fmt.Errorf("unsupported target type: %T", target)
	}
	return nil
}
