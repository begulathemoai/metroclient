# metroclient

a client written in go that connects to metrolist's listen together<br>
for now it cannot really be used as a module and is hardcoded to connect to my selfhosted metroserver instance<br>
input a room's code in stdin to connect to it<br>
<br>
quite a bit of code was taken from [metroserver](https://github.com/MetrolistGroup/metroserver)

# TODO list

## Server-to-client calls
### Room management
- [x] `RoomCreatedPayload` (the response for room creation)<br>
- [x] `JoinRequestPayload` (sent to the host when someone wants to join)<br>
- [x] `JoinApprovedPayload` (sent to the user when they are approved)<br>
- [x] `JoinRejectedPayload` (sent to the user when they are rejected)<br>
- [x] `UserJoinedPayload` (sent when a user joins the room)<br>
- [x] `UserLeftPayload` (sent when a user leaves the room)<br>
- [ ] `KickedPayload` (sent to the user when they are kicked)<br>
- [ ] `HostChangedPayload` (sent when the host changes)<br>
- [ ] `ReconnectedPayload` (sent when successfully reconnected)<br>
- [ ] `UserReconnectedPayload` (sent to other users when someone reconnects)<br>
- [ ] `UserDisconnectedPayload` (sent when a user temporarily disconnects)<br>

### Playback
- [ ] `PlaybackActionPayload` (for playback control actions)<br>
- [x] `SyncStatePayload` (sent to a guest when they request current playback state)<br>

### Song requests
- [ ] `SuggestionReceivedPayload` (sent to the host when a song suggestion is received)<br>
- [ ] `SuggestionApprovedPayload` (sent to a guest when their song suggestion is approved)<br>
- [ ] `SuggestionRejectedPayload` (sent to a guest when their song suggestion is denied)<br>

### Misc
- [x] `ErrorPayload` (for error messages)<br>
- [x] `ServerCapabilitiesPayload` (client-server handshake)<br>
- [ ] `BufferReadyPayload` (sent when a user has finished buffering)<br>
- [ ] `BufferWaitPayload` (sent to tell users to wait for buffering)<br>
- [ ] `BufferCompletePayload` (sent when all users have buffered)<br>

## Client-to-server calls
### Room management
- [x] `CreateRoomPayload` (for creating a new room)<br>
- [x] `JoinRoomPayload` (for joining a room)<br>
- [x] `ApproveJoinPayload` (for approving a join request)<br>
- [ ] `RejectJoinPayload` (for rejecting a join request)<br>
- [ ] `ReconnectPayload` (for reconnecting to a room)<br>
- [ ] `TransferHostPayload` (for transferring host role to another user)<br>
- [ ] `KickUserPayload` (for kicking a user from the room)<br>

### Playback
- [ ] `PlaybackActionPayload` (for playback control actions)<br>

### Song requests
- [ ] `SuggestTrackPayload` (for suggesting a track to the host)<br>
- [ ] `ApproveSuggestionPayload` (for approving a song suggestion)<br>
- [ ] `RejectSuggestionPayload` (for rejecting a song suggestion)<br>

### Misc
- [ ] `ClientCapabilitiesPayload` (client-server handshake)<br>
