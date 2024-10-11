package dicedb

import (
	"context"
	"fmt"
	"strconv"
)

// ZRangeWatch wraps the WatchCommand to handle ZRANGE responses with strong typing.
type ZRangeWatch struct {
	watchCommand *WatchCommand
}

type ZRangeWatchNotification struct {
	Command     string
	Fingerprint string
	Data        []Z
}

func (z *ZRangeWatchNotification) String() string {
	return fmt.Sprintf("ZRangeWatchNotification(Command=%v, Fingerprint=%v, Data=%v)", z.Command, z.Fingerprint, z.Data)
}

// NewZRangeWatch creates a new ZRangeWatch.
func NewZRangeWatch(ctx context.Context, client *Client) (*ZRangeWatch, error) {
	watch := client.WatchCommand(ctx)
	if watch == nil {
		return nil, fmt.Errorf("failed to create watch command")
	}

	return &ZRangeWatch{watchCommand: watch}, nil
}

// Watch starts watching the ZRANGE command for the specified key.
func (z *ZRangeWatch) Watch(ctx context.Context, args ...interface{}) (*ZRangeWatchNotification, error) {
	// Use the ZRANGE command with necessary arguments.
	firstMsg, err := z.watchCommand.Watch(ctx, "ZRANGE", args...)
	if err != nil {
		return nil, fmt.Errorf("failed to start ZRANGE watch: %v", err)
	}

	// Parse the first message's data into Score.
	return z.parseScores(firstMsg)
}

// Channel returns a channel for receiving subsequent ZRANGE messages with strong typing.
func (z *ZRangeWatch) Channel() <-chan *ZRangeWatchNotification {
	msgCh := z.watchCommand.Channel()
	scoreCh := make(chan *ZRangeWatchNotification)

	go func() {
		for msg := range msgCh {
			scores, err := z.parseScores(msg)
			if err == nil {
				scoreCh <- scores
			}
		}
		close(scoreCh)
	}()

	return scoreCh
}

// parseScores parses the Data from ZRANGE into a slice of Score.
func (z *ZRangeWatch) parseScores(watchNotification *WatchNotification) (*ZRangeWatchNotification, error) {
	dataList, ok := watchNotification.Data.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected data type in ZRANGE message")
	}

	var scores []Z
	for i := 0; i < len(dataList); i += 2 {
		member, ok1 := dataList[i].(string)
		scoreStr, ok2 := dataList[i+1].(string)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("unexpected data types in ZRANGE message")
		}
		scoreFloat, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			return nil, err
		}
		scores = append(scores, Z{
			Member: member,
			Score:  scoreFloat,
		})
	}
	return &ZRangeWatchNotification{
		Command:     watchNotification.Command,
		Fingerprint: watchNotification.Fingerprint,
		Data:        scores,
	}, nil
}

// Close closes the underlying WatchCommand.
func (z *ZRangeWatch) Close() error {
	return z.watchCommand.Close()
}
