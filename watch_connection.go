package dicedb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dicedb/dicedb-go/internal"
	"github.com/dicedb/dicedb-go/internal/pool"
	"github.com/dicedb/dicedb-go/internal/proto"
	"github.com/google/uuid"
)

// WatchResult represents a message received via WatchConn.
type WatchResult struct {
	Command     string
	Fingerprint string
	Data        interface{}
}

func (m *WatchResult) String() string {
	return fmt.Sprintf("WatchResult(Command=%v, Fingerprint=%v, Data=%v)", m.Command, m.Fingerprint, m.Data)
}

// WatchConn implements the WATCHCOMMAND, which allows clients to watch commands.
// WatchResult receiving is NOT safe for concurrent use by multiple goroutines.
//
// WatchConn automatically reconnects to the Redis server and re-subscribes
// to the commands in case of network errors.
type WatchConn struct {
	opt *Options

	newConn   func(ctx context.Context, cmdName string, args ...interface{}) (*pool.Conn, error)
	closeConn func(*pool.Conn) error

	mu       sync.Mutex
	cn       *pool.Conn
	commands map[string][]interface{}
	closed   bool
	exit     chan struct{}

	cmd *Cmd

	chOnce sync.Once
	msgCh  *wChannel
}

func (w *WatchConn) init() {
	w.exit = make(chan struct{})
}

func (w *WatchConn) String() string {
	var sb strings.Builder
	for cmdName, args := range w.commands {
		sb.WriteString(fmt.Sprintf("%s(%v); ", cmdName, args))
	}
	return fmt.Sprintf("WatchConn(%s)", sb.String())
}

func (w *WatchConn) connWithLock(ctx context.Context, cmdName string, args ...interface{}) (*pool.Conn, error) {
	w.mu.Lock()
	cn, err := w.conn(ctx, cmdName, args...)
	w.mu.Unlock()
	return cn, err
}

func (w *WatchConn) conn(ctx context.Context, cmdName string, args ...interface{}) (*pool.Conn, error) {
	if w.closed {
		return nil, pool.ErrClosed
	}
	if w.cn != nil {
		return w.cn, nil
	}

	cn, err := w.newConn(ctx, cmdName, args...)
	if err != nil {
		return nil, err
	}

	if err := w.resubscribe(ctx, cn); err != nil {
		_ = w.closeConn(cn)
		return nil, err
	}

	w.cn = cn
	return cn, nil
}

// writeCmd writes a command to the connection.
func (w *WatchConn) writeCmd(ctx context.Context, cn *pool.Conn, cmd Cmder) error {
	return cn.WithWriter(ctx, w.opt.WriteTimeout, func(wr *proto.Writer) error {
		return writeCmd(wr, cmd)
	})
}

// resubscribe re-subscribes to all commands on a new connection.
func (w *WatchConn) resubscribe(ctx context.Context, cn *pool.Conn) error {
	var firstErr error

	for cmdName, args := range w.commands {
		err := w._watchCommand(ctx, cn, fmt.Sprintf("%s.WATCH", strings.ToUpper(cmdName)), args...)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// _watchCommand sends a WATCHCOMMAND command to the Redis server.
func (w *WatchConn) _watchCommand(ctx context.Context, cn *pool.Conn, cmdName string, args ...interface{}) error {
	cmdArgs := make([]interface{}, 0, 2+len(args))
	cmdArgs = append(cmdArgs, cmdName)
	cmdArgs = append(cmdArgs, args...)
	cmd := NewSliceCmd(ctx, cmdArgs...)
	return w.writeCmd(ctx, cn, cmd)
}

func (w *WatchConn) releaseConnWithLock(ctx context.Context, cn *pool.Conn, err error, allowTimeout bool) {
	w.mu.Lock()
	w.releaseConn(ctx, cn, err, allowTimeout)
	w.mu.Unlock()
}

func (w *WatchConn) releaseConn(ctx context.Context, cn *pool.Conn, err error, allowTimeout bool) {
	if w.cn != cn {
		return
	}
	if isBadConn(err, allowTimeout, w.opt.Addr) {
		w.reconnect(ctx, err)
	}
}

// reconnect closes the current connection and attempts to establish a new one.
// It must be called with the mutex locked.
func (w *WatchConn) reconnect(ctx context.Context, reason error) {
	_ = w.closeTheCn(reason)
	_, _ = w.conn(ctx, "")
}

// closeTheCn closes the current connection.
// It must be called with the mutex locked.
func (w *WatchConn) closeTheCn(reason error) error {
	if w.cn == nil {
		return nil
	}
	if !w.closed {
		w.closed = true
	}
	err := w.closeConn(w.cn)
	w.cn = nil
	return err
}

// Close closes the WatchConn instance.
func (w *WatchConn) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return pool.ErrClosed
	}
	w.closed = true
	close(w.exit)

	return w.closeTheCn(pool.ErrClosed)
}

// Watch subscribes the client to the specified command.
// It returns an error if subscription fails.
func (w *WatchConn) Watch(ctx context.Context, cmdName string, args ...interface{}) (*WatchResult, error) {
	w.mu.Lock()

	w.chOnce.Do(func() {
		w.msgCh = newWatchCommandChannel(w)
		w.msgCh.initMsgChan(ctx)
	})
	// create a unique watch label for this request
	watchLabel := uuid.New().String()
	args = append(args, watchLabel)
	// create a dedicated firstMsgCh for this request
	firstMsgCh := make(chan *WatchResult, 1)
	w.msgCh.watchLabelFirstMsgChMap[watchLabel] = firstMsgCh

	// Subscribe to the command
	err := w.watchCommand(ctx, cmdName, args...)
	if err != nil {
		return nil, err
	}

	if w.commands == nil {
		w.commands = make(map[string][]interface{})
	}
	w.commands[cmdName] = args

	w.mu.Unlock()

	// Get the first message synchronously to return it to the user.
	select {
	case firstMsg, ok := <-firstMsgCh:
		if !ok {
			return nil, fmt.Errorf("connection closed before receiving first update")
		}
		firstMsg.Command = cmdName // mask label from the user
		// cleanup
		close(firstMsgCh)
		delete(w.msgCh.watchLabelFirstMsgChMap, watchLabel)
		if firstMsg.Data, err = parseWithTypes(cmdName, firstMsg.Data); err != nil {
			return nil, err
		}
		return firstMsg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (w *WatchConn) GetWatch(ctx context.Context, args ...interface{}) (*WatchResult, error) {
	return w.Watch(ctx, "GET", args...)
}

func (w *WatchConn) ZRangeWatch(ctx context.Context, args ...interface{}) (*WatchResult, error) {
	return w.Watch(ctx, "ZRANGE", args...)
}

// watchCommand sends the WATCH command to the server.
// It must be called with the mutex locked.
func (w *WatchConn) watchCommand(ctx context.Context, cmdName string, args ...interface{}) error {
	cn, err := w.conn(ctx, cmdName, args...)
	if err != nil {
		return err
	}

	err = w._watchCommand(ctx, cn, fmt.Sprintf("%s.WATCH", strings.ToUpper(cmdName)), args...)
	w.releaseConn(ctx, cn, err, false)
	return err
}

// newWMessage processes the reply from the Redis server and constructs a message.
func (w *WatchConn) newWMessage(reply interface{}) (interface{}, error) {
	switch reply := reply.(type) {
	case string:
		return &Pong{Payload: reply}, nil
	case []interface{}:
		if len(reply) == 0 {
			return nil, fmt.Errorf("err: empty watchcommand message")
		}

		kind, ok := reply[0].(string)
		if !ok {
			return nil, fmt.Errorf("err: expected message type as string, got %T", reply[0])
		}

		if kind == "pong" {
			return parsePongMessage(reply)
		} else {
			return w.processWatchResult(reply)
		}
	default:
		return nil, fmt.Errorf("err: unsupported watchcommand message type: %T", reply)
	}
}

// processWatchResult parses a WATCHCOMMAND message from the server.
func (w *WatchConn) processWatchResult(payload []interface{}) (*WatchResult, error) {
	if len(payload) < 3 {
		return nil, fmt.Errorf("err: invalid watchcommand message format")
	}

	// Ensure command is a string
	command, ok := payload[0].(string)
	if !ok {
		return nil, fmt.Errorf("err: invalid command in watchcommand message, expected string, got %T", payload[0])
	}

	// Ensure name is a string
	fingerprint, ok := payload[1].(string)
	if !ok {
		return nil, fmt.Errorf("err: invalid fingerprint in watchcommand message, expected string, got %T", payload[1])
	}

	data := payload[2]
	typedData, err := parseWithTypes(command, data)
	if err != nil {
		return nil, err
	}

	return &WatchResult{Command: command, Fingerprint: fingerprint, Data: typedData}, nil
}

func parseWithTypes(cmd string, data interface{}) (interface{}, error) {
	switch cmd {
	case "GET.WATCH", "GET":
		return data, nil
	case "ZRANGE.WATCH", "ZRANGE":
		return parseZRangeResult(data)
	default:
		return data, nil
	}
}

// parseZRangeResult parses the Data from ZRANGE or ZRANGE.WATCH into appropriate type
func parseZRangeResult(data interface{}) (interface{}, error) {
	dataList, ok := data.([]interface{})
	if !ok {
		return nil, nil
	}

	// Empty result case
	if len(dataList) == 0 {
		return []string{}, nil
	}

	// Check if we have scores by examining ALL potential score positions
	// Only consider it a WITHSCORES result if all even-indexed elements are valid floats
	hasScores := len(dataList) > 1 && len(dataList)%2 == 0 // must have even number of elements
	if hasScores {
		// Check every alternate position (potential score positions)
		for i := 1; i < len(dataList); i += 2 {
			scoreStr, ok := dataList[i].(string)
			if !ok {
				hasScores = false
				break
			}
			if _, err := strconv.ParseFloat(scoreStr, 64); err != nil {
				hasScores = false
				break
			}
		}
	}

	if hasScores {
		return parseZRangeWithScores(dataList)
	}
	return parseZRangeMembers(dataList)
}

// parseZRangeWithScores parses when WITHSCORES was used
func parseZRangeWithScores(dataList []interface{}) ([]Z, error) {
	if len(dataList)%2 != 0 {
		return nil, fmt.Errorf("err: invalid ZRANGE.WATCH message format")
	}

	scores := make([]Z, 0, len(dataList)/2)
	for i := 0; i < len(dataList); i += 2 {
		member, ok1 := dataList[i].(string)
		scoreStr, ok2 := dataList[i+1].(string)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("err: invalid ZRANGE.WATCH message format")
		}
		scoreFloat, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			return nil, fmt.Errorf("err: invalid ZRANGE.WATCH message format")
		}
		scores = append(scores, Z{
			Member: member,
			Score:  scoreFloat,
		})
	}
	return scores, nil
}

// parseZRangeMembers parses when WITHSCORES was not used
func parseZRangeMembers(dataList []interface{}) ([]string, error) {
	members := make([]string, 0, len(dataList))
	for _, item := range dataList {
		member, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("err: invalid ZRANGE.WATCH message format")
		}
		members = append(members, member)
	}
	return members, nil
}

// ReceiveTimeout acts like Receive but returns an error if a message
// is not received in time. This is a low-level API and in most cases
// Channel should be used instead.
func (w *WatchConn) ReceiveTimeout(ctx context.Context, timeout time.Duration) (interface{}, error) {
	if w.cmd == nil {
		w.cmd = NewCmd(ctx)
	}

	cn, err := w.connWithLock(ctx, "")
	if err != nil {
		return nil, err
	}

	err = cn.WithReader(ctx, timeout, func(rd *proto.Reader) error {
		return w.cmd.readReply(rd)
	})

	w.releaseConnWithLock(ctx, cn, err, timeout > 0)

	if err != nil {
		return nil, err
	}

	return w.newWMessage(w.cmd.Val())
}

// Receive returns a message as a WatchResult, Pong, or error.
// This is a low-level API and in most cases Channel should be used instead.
func (w *WatchConn) Receive(ctx context.Context) (interface{}, error) {
	return w.ReceiveTimeout(ctx, 0)
}

// ReceiveWMessage returns a WatchResult or error, ignoring Pong messages.
// This is a low-level API and in most cases Channel should be used instead.
func (w *WatchConn) ReceiveWMessage(ctx context.Context) (*WatchResult, error) {
	for {
		msg, err := w.Receive(ctx)
		if err != nil {
			return nil, err
		}

		switch msg := msg.(type) {
		case *Pong:
			// Ignore.
		case *WatchResult:
			return msg, nil
		default:
			return nil, fmt.Errorf("err: unknown message type: %T", msg)
		}
	}
}

func (w *WatchConn) getContext() context.Context {
	if w.cmd != nil {
		return w.cmd.ctx
	}
	return context.Background()
}

// Channel returns a Go channel for concurrently receiving messages.
// The channel is closed together with the WatchConn. If the Go channel
// is blocked full for 1 minute, the message is dropped.
// Receive* APIs cannot be used after the channel is created.
//
// go-redis periodically sends ping messages to test connection health
// and re-subscribes if ping cannot be received for 1 minute.
func (w *WatchConn) Channel(opts ...WChannelOption) <-chan *WatchResult {
	w.chOnce.Do(func() {
		w.msgCh = newWatchCommandChannel(w, opts...)
		w.msgCh.initMsgChan(context.Background())
	})
	if w.msgCh == nil {
		err := fmt.Errorf("err: Channel can't be called after ChannelWithSubscriptions")
		panic(err)
	}
	return w.msgCh.updateCh
}

// wChannel handles message delivery over a Go channel.
type wChannel struct {
	watchCmd *WatchConn

	updateCh                chan *WatchResult // channel to receive all updates - first and subsequent
	ping                    chan struct{}
	watchLabelFirstMsgChMap map[string]chan *WatchResult

	chanSize        int
	chanSendTimeout time.Duration
	checkInterval   time.Duration
}

// WChannelOption configures a wChannel.
type WChannelOption func(c *wChannel)

// WithWChannelSize specifies the size of the Go channel buffer.
// The default is 100 messages.
func WithWChannelSize(size int) WChannelOption {
	return func(c *wChannel) {
		c.chanSize = size
	}
}

// WithWChannelHealthCheckInterval specifies the health check interval.
// WatchConn will ping the Redis server if it does not receive any messages within the interval.
// To disable health check, use zero interval.
// The default is 3 seconds.
func WithWChannelHealthCheckInterval(d time.Duration) WChannelOption {
	return func(c *wChannel) {
		c.checkInterval = d
	}
}

// WithWChannelSendTimeout specifies the timeout for sending messages to the Go channel.
// If the timeout is exceeded, the message is dropped.
// The default is 60 seconds.
func WithWChannelSendTimeout(d time.Duration) WChannelOption {
	return func(c *wChannel) {
		c.chanSendTimeout = d
	}
}

// newWatchCommandChannel creates a new wChannel.
func newWatchCommandChannel(watchCmd *WatchConn, opts ...WChannelOption) *wChannel {
	c := &wChannel{
		watchCmd:        watchCmd,
		chanSize:        100,
		chanSendTimeout: time.Minute,
		checkInterval:   3 * time.Second,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.checkInterval > 0 {
		c.initHealthCheck()
	}
	return c
}

// Ping sends a PING command to the server to check connection health.
func (w *WatchConn) Ping(ctx context.Context, payload ...string) error {
	args := []interface{}{"ping"}
	if len(payload) == 1 {
		args = append(args, payload[0])
	}
	cmd := NewCmd(ctx, args...)

	w.mu.Lock()
	defer w.mu.Unlock()

	cn, err := w.conn(ctx, "")
	if err != nil {
		return err
	}

	err = w.writeCmd(ctx, cn, cmd)
	w.releaseConn(ctx, cn, err, false)
	return err
}

// initHealthCheck initializes the health check routine.
func (c *wChannel) initHealthCheck() {
	ctx := context.TODO()
	c.ping = make(chan struct{}, 1)

	go func() {
		timer := time.NewTimer(time.Minute)
		timer.Stop()

		for {
			timer.Reset(c.checkInterval)
			select {
			case <-c.ping:
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
				if pingErr := c.watchCmd.Ping(ctx); pingErr != nil {
					c.watchCmd.mu.Lock()
					c.watchCmd.reconnect(ctx, pingErr)
					c.watchCmd.mu.Unlock()
				}
			case <-c.watchCmd.exit:
				return
			}
		}
	}()
}

// initMsgChan initializes the message receiving routine.
// It routes the first response to the dedicated channel and subsequent responses to the update channel.
func (c *wChannel) initMsgChan(ctx context.Context) {
	c.updateCh = make(chan *WatchResult, c.chanSize)
	c.watchLabelFirstMsgChMap = make(map[string]chan *WatchResult)

	go c.receiveMessages(ctx)
}

// receiveMessages handles incoming messages in a separate goroutine.
func (c *wChannel) receiveMessages(ctx context.Context) {
	var errCount int

	for {
		msg, err := c.watchCmd.Receive(ctx)
		if err != nil {
			if errors.Is(err, pool.ErrClosed) {
				close(c.updateCh)
				return
			}
			errCount++
			if errCount > 1 {
				time.Sleep(100 * time.Millisecond)
			}
			continue
		}
		errCount = 0

		// Any message is as good as a ping.
		select {
		case c.ping <- struct{}{}:
		default:
		}

		switch msg := msg.(type) {
		case *Pong:
			// Ignore pong messages.
		case *WatchResult:
			c.handleWatchResult(ctx, msg)
		default:
			internal.Logger.Printf(ctx, "error: unknown message type: %T", msg)
		}
	}
}

// handleWatchResult processes WatchResult messages, routing them to the appropriate channels.
func (c *wChannel) handleWatchResult(ctx context.Context, msg *WatchResult) {
	// Try to parse msg.Command as a UUID to determine if it's the first message.
	if label, err := uuid.Parse(msg.Command); err == nil {
		// It's the first message.
		firstMsgCh, ok := c.watchLabelFirstMsgChMap[label.String()]
		if !ok || firstMsgCh == nil {
			internal.Logger.Printf(ctx, "redis: first message channel not found for %s", label.String())
			return
		}
		// Try to send the message to firstMsgCh with timeout.
		select {
		case firstMsgCh <- msg:
		case <-time.After(c.chanSendTimeout):
			internal.Logger.Printf(ctx, "redis: channel is full for %s (message is dropped)", c.watchCmd)
		}
		return
	}

	// It's a subsequent message; route it to the update channel.
	select {
	case c.updateCh <- msg:
	case <-time.After(c.chanSendTimeout):
		internal.Logger.Printf(ctx, "error: channel is full for %s (message is dropped)", c.watchCmd)
	}
}

// Unwatch unsubscribes the client from the specified command.
// It returns an error if unsubscription fails.
func (w *WatchConn) Unwatch(ctx context.Context, cmdName string, args ...interface{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	err := w.unwatchCommand(ctx, cmdName, args...)
	if err == nil {
		delete(w.commands, cmdName)
	}
	return err
}

// unwatchCommand sends the .UNWATCH command to the server.
// It must be called with the mutex locked.
func (w *WatchConn) unwatchCommand(ctx context.Context, cmdName string, args ...interface{}) error {
	cn, err := w.conn(ctx, cmdName)
	if err != nil {
		return err
	}

	err = w._unwatchCommand(ctx, cn, fmt.Sprintf("%s.UNWATCH", strings.ToUpper(cmdName)), args...)
	w.releaseConn(ctx, cn, err, false)
	return err
}

// _unwatchCommand sends the .UNWATCH command to the Redis server.
func (w *WatchConn) _unwatchCommand(ctx context.Context, cn *pool.Conn, cmdName string, args ...interface{}) error {
	cmdArgs := make([]interface{}, 0, 2+len(args))
	cmdArgs = append(cmdArgs, cmdName)
	cmdArgs = append(cmdArgs, args...)
	cmd := NewSliceCmd(ctx, cmdArgs...)
	return w.writeCmd(ctx, cn, cmd)
}
