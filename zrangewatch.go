package dicedb

import (
	"context"
	"fmt"
	"strconv"
)

// ZRangeWatch wraps the WatchConn to handle ZRANGE responses with strong typing.
type ZRangeWatch struct {
	watchConn *WatchConn
}

type ZRangeWatchResult struct {
	Command     string
	Fingerprint string
	Data        []Z
}

func (z *ZRangeWatchResult) String() string {
	return fmt.Sprintf("ZRangeWatchResult(Command=%v, Fingerprint=%v, Data=%v)", z.Command, z.Fingerprint, z.Data)
}

// NewZRangeWatch creates a new ZRangeWatch.
func NewZRangeWatch(ctx context.Context, client *Client) (*ZRangeWatch, error) {
	watch := client.WatchConn(ctx)
	if watch == nil {
		return nil, fmt.Errorf("failed to create watch command")
	}

	return &ZRangeWatch{watchConn: watch}, nil
}

// Watch starts watching the ZRANGE command for the specified key.
func (z *ZRangeWatch) Watch(ctx context.Context, args ...interface{}) (*ZRangeWatchResult, error) {
	// Use the ZRANGE command with necessary arguments.
	firstMsg, err := z.watchConn.Watch(ctx, "ZRANGE", args...)
	if err != nil {
		return nil, fmt.Errorf("failed to start ZRANGE watch: %v", err)
	}

	// Parse the first message's data into Score.
	return z.parseScores(firstMsg)
}

// Channel returns a channel for receiving subsequent ZRANGE messages with strong typing.
func (z *ZRangeWatch) Channel() <-chan *ZRangeWatchResult {
	msgCh := z.watchConn.Channel()
	scoreCh := make(chan *ZRangeWatchResult)

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
func (z *ZRangeWatch) parseScores(watchNotification *WatchResult) (*ZRangeWatchResult, error) {
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
	return &ZRangeWatchResult{
		Command:     watchNotification.Command,
		Fingerprint: watchNotification.Fingerprint,
		Data:        scores,
	}, nil
}

// Close closes the underlying WatchConn.
func (z *ZRangeWatch) Close() error {
	return z.watchConn.Close()
}
