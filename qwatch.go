package dicedb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dicedb/go-dice/internal"
	"github.com/dicedb/go-dice/internal/pool"
	"github.com/dicedb/go-dice/internal/proto"
)

// KV represents a key-value pair.
type KV struct {
	Key   string
	Value interface{}
}

// QMessage represents a message received via QWATCH.
type QMessage struct {
	Command string
	Query   string
	Updates []KV
}

func (m *QMessage) String() string {
	return fmt.Sprintf("QMessage(%v)", m.Updates)
}

// QWatch implements the QWATCH command, which allows clients to watch queries.
// QMessage receiving is NOT safe for concurrent use by multiple goroutines.
//
// QWatch automatically reconnects to the Redis server and re-subscribes
// to the queries in case of network errors.
type QWatch struct {
	opt *Options

	newConn   func(ctx context.Context, query string, args ...interface{}) (*pool.Conn, error)
	closeConn func(*pool.Conn) error

	mu      sync.Mutex
	cn      *pool.Conn
	queries map[string][]interface{}
	closed  bool
	exit    chan struct{}

	cmd *Cmd

	chOnce sync.Once
	msgCh  *qChannel
}

func (q *QWatch) init() {
	q.exit = make(chan struct{})
}

func (q *QWatch) String() string {
	var sb strings.Builder
	for query, args := range q.queries {
		sb.WriteString(fmt.Sprintf("%s(%v); ", query, args))
	}
	return fmt.Sprintf("QWatch(%s)", sb.String())
}

func (q *QWatch) connWithLock(ctx context.Context, query string, args ...interface{}) (*pool.Conn, error) {
	q.mu.Lock()
	cn, err := q.conn(ctx, query, args...)
	q.mu.Unlock()
	return cn, err
}

func (q *QWatch) conn(ctx context.Context, query string, args ...interface{}) (*pool.Conn, error) {
	if q.closed {
		return nil, pool.ErrClosed
	}
	if q.cn != nil {
		return q.cn, nil
	}

	cn, err := q.newConn(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	if err := q.resubscribe(ctx, cn); err != nil {
		_ = q.closeConn(cn)
		return nil, err
	}

	q.cn = cn
	return cn, nil
}

// writeCmd writes a command to the connection.
func (q *QWatch) writeCmd(ctx context.Context, cn *pool.Conn, cmd Cmder) error {
	return cn.WithWriter(context.Background(), q.opt.WriteTimeout, func(wr *proto.Writer) error {
		return writeCmd(wr, cmd)
	})
}

// resubscribe re-subscribes to all queries on a new connection.
func (q *QWatch) resubscribe(ctx context.Context, cn *pool.Conn) error {
	var firstErr error

	for query, args := range q.queries {
		err := q._watchQuery(ctx, cn, "qwatch", query, args...)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// _watchQuery sends a QWATCH command to the Redis server.
func (q *QWatch) _watchQuery(ctx context.Context, cn *pool.Conn, redisCmd string, query string, args ...interface{}) error {
	cmdArgs := make([]interface{}, 0, 2+len(args))
	cmdArgs = append(cmdArgs, redisCmd, query)
	cmdArgs = append(cmdArgs, args...)
	cmd := NewSliceCmd(ctx, cmdArgs...)
	return q.writeCmd(ctx, cn, cmd)
}

func (q *QWatch) releaseConnWithLock(ctx context.Context, cn *pool.Conn, err error, allowTimeout bool) {
	q.mu.Lock()
	q.releaseConn(ctx, cn, err, allowTimeout)
	q.mu.Unlock()
}

func (q *QWatch) releaseConn(ctx context.Context, cn *pool.Conn, err error, allowTimeout bool) {
	if q.cn != cn {
		return
	}
	if isBadConn(err, allowTimeout, q.opt.Addr) {
		q.reconnect(ctx, err)
	}
}

// reconnect closes the current connection and attempts to establish a new one.
// It must be called with the mutex locked.
func (q *QWatch) reconnect(ctx context.Context, reason error) {
	_ = q.closeTheCn(reason)
	_, _ = q.conn(ctx, "")
}

// closeTheCn closes the current connection.
// It must be called with the mutex locked.
func (q *QWatch) closeTheCn(reason error) error {
	if q.cn == nil {
		return nil
	}
	if !q.closed {
		internal.Logger.Printf(q.getContext(), "redis: discarding bad QWatch connection: %s", reason)
	}
	err := q.closeConn(q.cn)
	q.cn = nil
	return err
}

// Close closes the QWatch instance.
func (q *QWatch) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return pool.ErrClosed
	}
	q.closed = true
	close(q.exit)

	return q.closeTheCn(pool.ErrClosed)
}

// WatchQuery subscribes the client to the specified query.
// It returns an error if subscription fails.
func (q *QWatch) WatchQuery(ctx context.Context, query string, args ...interface{}) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	err := q.watchQuery(ctx, "QWATCH", query, args...)
	if err != nil {
		return err
	}

	if q.queries == nil {
		q.queries = make(map[string][]interface{})
	}
	q.queries[query] = args

	return nil
}

// watchQuery sends the QWATCH command to the server.
// It must be called with the mutex locked.
func (q *QWatch) watchQuery(ctx context.Context, redisCmd string, query string, args ...interface{}) error {
	cn, err := q.conn(ctx, query, args...)
	if err != nil {
		return err
	}

	err = q._watchQuery(ctx, cn, redisCmd, query, args...)
	q.releaseConn(ctx, cn, err, false)
	return err
}

// newQMessage processes the reply from the Redis server and constructs a message.
func (q *QWatch) newQMessage(reply interface{}) (interface{}, error) {
	switch reply := reply.(type) {
	case string:
		return &Pong{Payload: reply}, nil
	case []interface{}:
		if len(reply) == 0 {
			return nil, fmt.Errorf("redis: empty qwatch message")
		}

		kind, ok := reply[0].(string)
		if !ok {
			return nil, fmt.Errorf("redis: expected message type as string, got %T", reply[0])
		}

		switch kind {
		case "qwatch":
			return q.processQWatchMessage(reply)
		case "pong":
			return parsePongMessage(reply)
		default:
			return nil, fmt.Errorf("redis: unsupported qwatch message: %q", kind)
		}
	default:
		return nil, fmt.Errorf("redis: unsupported qwatch message type: %T", reply)
	}
}

// parsePongMessage parses a PONG message from the server.
func parsePongMessage(reply []interface{}) (*Pong, error) {
	if len(reply) < 2 {
		return nil, fmt.Errorf("redis: invalid pong message format")
	}

	payload, ok := reply[1].(string)
	if !ok {
		return nil, fmt.Errorf("redis: invalid pong payload type")
	}

	return &Pong{Payload: payload}, nil
}

// processQWatchMessage parses a QWATCH message from the server.
func (q *QWatch) processQWatchMessage(payload []interface{}) (*QMessage, error) {
	if len(payload) < 3 {
		return nil, fmt.Errorf("redis: invalid qwatch message format")
	}

	// Ensure command is a string
	command, ok := payload[0].(string)
	if !ok {
		return nil, fmt.Errorf("redis: invalid command in qwatch message, expected string, got %T", payload[0])
	}

	// Ensure query is a string
	query, ok := payload[1].(string)
	if !ok {
		return nil, fmt.Errorf("redis: invalid query in qwatch message, expected string, got %T", payload[1])
	}

	updates, err := parseUpdates(payload[2])
	if err != nil {
		return nil, err
	}

	return &QMessage{Command: command, Query: query, Updates: updates}, nil
}

// parseUpdates parses the updates from the QWATCH message.
func parseUpdates(data interface{}) ([]KV, error) {
	updateList, ok := data.([]interface{})
	if !ok {
		return nil, fmt.Errorf("redis: invalid update list format, expected []interface{}, got %T", data)
	}

	updates := make([]KV, 0, len(updateList))
	for _, update := range updateList {
		kv, err := parseKeyValuePair(update)
		if err != nil {
			return nil, err
		}
		updates = append(updates, kv)
	}

	return updates, nil
}

// parseKeyValuePair parses a key-value pair from the updates.
func parseKeyValuePair(update interface{}) (KV, error) {
	pair, ok := update.([]interface{})
	if !ok || len(pair) != 2 {
		return KV{}, fmt.Errorf("redis: invalid key-value pair format")
	}

	// Ensure key is a string
	key, ok := pair[0].(string)
	if !ok {
		return KV{}, fmt.Errorf("redis: invalid key type")
	}

	// Value can be any type
	value := pair[1]

	return KV{Key: key, Value: value}, nil
}

// ReceiveTimeout acts like Receive but returns an error if a message
// is not received in time. This is a low-level API and in most cases
// Channel should be used instead.
func (q *QWatch) ReceiveTimeout(ctx context.Context, timeout time.Duration) (interface{}, error) {
	if q.cmd == nil {
		q.cmd = NewCmd(ctx)
	}

	cn, err := q.connWithLock(ctx, "")
	if err != nil {
		return nil, err
	}

	err = cn.WithReader(context.Background(), timeout, func(rd *proto.Reader) error {
		return q.cmd.readReply(rd)
	})

	q.releaseConnWithLock(ctx, cn, err, timeout > 0)

	if err != nil {
		return nil, err
	}

	return q.newQMessage(q.cmd.Val())
}

// Receive returns a message as a QMessage, Pong, or error.
// This is a low-level API and in most cases Channel should be used instead.
func (q *QWatch) Receive(ctx context.Context) (interface{}, error) {
	return q.ReceiveTimeout(ctx, 0)
}

// ReceiveQMessage returns a QMessage or error, ignoring Pong messages.
// This is a low-level API and in most cases Channel should be used instead.
func (q *QWatch) ReceiveQMessage(ctx context.Context) (*QMessage, error) {
	for {
		msg, err := q.Receive(ctx)
		if err != nil {
			return nil, err
		}

		switch msg := msg.(type) {
		case *Pong:
			// Ignore.
		case *QMessage:
			return msg, nil
		default:
			return nil, fmt.Errorf("redis: unknown message type: %T", msg)
		}
	}
}

func (q *QWatch) getContext() context.Context {
	if q.cmd != nil {
		return q.cmd.ctx
	}
	return context.Background()
}

// Channel returns a Go channel for concurrently receiving messages.
// The channel is closed together with the QWatch. If the Go channel
// is blocked full for 1 minute, the message is dropped.
// Receive* APIs cannot be used after the channel is created.
//
// go-redis periodically sends ping messages to test connection health
// and re-subscribes if ping cannot be received for 1 minute.
func (q *QWatch) Channel(opts ...QChannelOption) <-chan *QMessage {
	q.chOnce.Do(func() {
		q.msgCh = newWatchChannel(q, opts...)
		q.msgCh.initMsgChan()
	})
	if q.msgCh == nil {
		err := fmt.Errorf("redis: Channel can't be called after ChannelWithSubscriptions")
		panic(err)
	}
	return q.msgCh.msgCh
}

// qChannel handles message delivery over a Go channel.
type qChannel struct {
	qwatch *QWatch

	msgCh chan *QMessage
	allCh chan interface{}
	ping  chan struct{}

	chanSize        int
	chanSendTimeout time.Duration
	checkInterval   time.Duration
}

// QChannelOption configures a qChannel.
type QChannelOption func(c *qChannel)

// WithQChannelSize specifies the size of the Go channel buffer.
// The default is 100 messages.
func WithQChannelSize(size int) QChannelOption {
	return func(c *qChannel) {
		c.chanSize = size
	}
}

// WithQChannelHealthCheckInterval specifies the health check interval.
// QWatch will ping the Redis server if it does not receive any messages within the interval.
// To disable health check, use zero interval.
// The default is 3 seconds.
func WithQChannelHealthCheckInterval(d time.Duration) QChannelOption {
	return func(c *qChannel) {
		c.checkInterval = d
	}
}

// WithQChannelSendTimeout specifies the timeout for sending messages to the Go channel.
// If the timeout is exceeded, the message is dropped.
// The default is 60 seconds.
func WithQChannelSendTimeout(d time.Duration) QChannelOption {
	return func(c *qChannel) {
		c.chanSendTimeout = d
	}
}

// newWatchChannel creates a new qChannel.
func newWatchChannel(qwatch *QWatch, opts ...QChannelOption) *qChannel {
	c := &qChannel{
		qwatch:          qwatch,
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
func (q *QWatch) Ping(ctx context.Context, payload ...string) error {
	args := []interface{}{"ping"}
	if len(payload) == 1 {
		args = append(args, payload[0])
	}
	cmd := NewCmd(ctx, args...)

	q.mu.Lock()
	defer q.mu.Unlock()

	cn, err := q.conn(ctx, "")
	if err != nil {
		return err
	}

	err = q.writeCmd(ctx, cn, cmd)
	q.releaseConn(ctx, cn, err, false)
	return err
}

// initHealthCheck initializes the health check routine.
func (c *qChannel) initHealthCheck() {
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
				if pingErr := c.qwatch.Ping(ctx); pingErr != nil {
					c.qwatch.mu.Lock()
					c.qwatch.reconnect(ctx, pingErr)
					c.qwatch.mu.Unlock()
				}
			case <-c.qwatch.exit:
				return
			}
		}
	}()
}

// initMsgChan initializes the message receiving routine.
func (c *qChannel) initMsgChan() {
	ctx := context.TODO()
	c.msgCh = make(chan *QMessage, c.chanSize)

	go func() {
		timer := time.NewTimer(time.Minute)
		timer.Stop()

		var errCount int
		for {
			msg, err := c.qwatch.Receive(ctx)
			if err != nil {
				if errors.Is(err, pool.ErrClosed) {
					close(c.msgCh)
					return
				}
				if errCount > 0 {
					time.Sleep(100 * time.Millisecond)
				}
				errCount++
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
				// Ignore.
			case *QMessage:
				timer.Reset(c.chanSendTimeout)
				select {
				case c.msgCh <- msg:
					if !timer.Stop() {
						<-timer.C
					}
				case <-timer.C:
					internal.Logger.Printf(
						ctx, "redis: %s channel is full for %s (message is dropped)",
						c.qwatch, c.chanSendTimeout)
				}
			default:
				internal.Logger.Printf(ctx, "redis: unknown message type: %T", msg)
			}
		}
	}()
}

// UnwatchQuery unsubscribes the client from the specified query.
// It returns an error if unsubscription fails.
func (q *QWatch) UnwatchQuery(ctx context.Context, query string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	err := q.unwatchQuery(ctx, "QUNWATCH", query)
	if err == nil {
		delete(q.queries, query)
	}
	return err
}

// unwatchQuery sends the QUNWATCH command to the server.
// It must be called with the mutex locked.
func (q *QWatch) unwatchQuery(ctx context.Context, redisCmd string, query string) error {
	cn, err := q.conn(ctx, query)
	if err != nil {
		return err
	}

	err = q._unwatchQuery(ctx, cn, redisCmd, query)
	q.releaseConn(ctx, cn, err, false)
	return err
}

// _unwatchQuery sends the QUNWATCH command to the Redis server.
func (q *QWatch) _unwatchQuery(ctx context.Context, cn *pool.Conn, redisCmd string, query string) error {
	cmd := NewSliceCmd(ctx, redisCmd, query)
	return q.writeCmd(ctx, cn, cmd)
}
