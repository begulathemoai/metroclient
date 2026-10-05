package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"bufio"

	"github.com/begulathemoai/metroclient/pkg/metroclient"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func uinput_loop(ch *chan string) {
	reader := bufio.NewScanner(os.Stdin)
	for {

		if !reader.Scan() {
			c := time.After(time.Millisecond * 100)
			<-c
			continue
		}
		err := reader.Err()
		if err != nil {
			continue
		}
		*ch <- reader.Text()
	}

}

func process_input(input string, c *metroclient.Client) {
	tokens := strings.Split(input, " ")
	counter := 1
	switch strings.ToLower(tokens[0]) {
	case "create":
		c.CreateRoom()
	case "join":
		if len(tokens) < 2 {
			c.Logger.Error("Unknown input", zap.String("input", input))
			return
		}
		c.JoinRoom(tokens[counter])
	case "leave":
		err := c.LeaveRoom()
		if err != nil {
			c.Logger.Error("An error was encountered", zap.Error(err))
		}
	case "sync":
		fmt.Println("Requesting sync...")
		c.RequestSync()
	case "accept":
		for id, user := range c.RoomState.PendingJoinRequests {
			fmt.Printf("Accepting request from %v\n", user)
			c.AcceptJoinRequest(id)
		}
	case "info":

		if c.RoomState.RoomCode == "" {
			c.Logger.Info("You are not currently in a room.")
		} else {
			out := strings.Builder{}
			out.WriteString("Room state for room ")
			out.WriteString(c.RoomState.RoomCode)
			out.WriteString(" hosted by ")
			out.WriteString(c.RoomState.HostID)
			out.WriteString("\n")
			ucount := 0
			for _, user := range c.RoomState.Users {
				if user.Username != "" {
					ucount += 1
				}
			}
			out.WriteString("There are currently ")
			fmt.Fprintf(&out, "%v", ucount)
			out.WriteString(" users in this room.\nCurrent queue : ")
			out.WriteString(c.RoomState.QueueTitle)
			out.WriteString("\n")
			if c.RoomState.CurrentTrack != nil {
				out.WriteString("Now Playing : ")
				out.WriteString(c.RoomState.CurrentTrack.Title)
				fmt.Fprintf(&out, " (%.2f%%)\n", float64(c.RoomState.Position)/float64(c.RoomState.CurrentTrack.Duration)*100)
			}
			out.WriteString("Latest update was at ")
			out.WriteString(time.UnixMilli(c.RoomState.LastUpdate).Format(time.TimeOnly))
			out.WriteString(" (current time is ")
			out.WriteString(time.Now().Format(time.TimeOnly))
			out.WriteString(")\n")
			for _, user := range c.RoomState.Users {
				if user.Username == "" {
					continue
				}
				out.WriteString(user.Username)
				if !user.IsConnected {
					out.WriteString(" (disconnected)")
				}
				out.WriteString("\n")
			}
			fmt.Println(out.String())
		}
	default:
		c.Logger.Error("Unknown input", zap.String("input", input))
	}
}

func main() {
	config := zap.NewDevelopmentConfig()

	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.Level.SetLevel(zapcore.DebugLevel)
	logger, err := config.Build()

	if err != nil {
		fmt.Println("Could not set up logging")
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	c, err := metroclient.NewClient("wss://metroserverx.begulathemoai.dev/ws", logger)
	if err != nil {
		logger.Fatal("error : %v", zap.Error(err))
	}

	ch := make(chan string)
	go uinput_loop(&ch)

	running := true
	for running {
		select {
		case <-interrupt:
			running = false
		case input := <-ch:
			process_input(input, c)

		}

	}

	logger.Info("Closing connection...")
	c.Close()
}
