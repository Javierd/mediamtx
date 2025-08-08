package mse

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/logger"
)

const (
	closeCheckPeriod = 1 * time.Second
)

// instanceParent is implemented by muxer and consumed by muxerInstance
// to log and keep the muxer alive while clients are connected.
type instanceParent interface {
	logger.Writer
	touchActivity()
}

type muxerGetInstanceReq struct{ res chan *muxerInstance }

type muxer struct {
	parentCtx   context.Context
	remoteAddr  string
	closeAfter  conf.Duration
	wg          *sync.WaitGroup
	pathName    string
	pathManager serverPathManager
	parent      *Server
	query       string

	ctx             context.Context
	ctxCancel       func()
	created         time.Time
	path            defs.Path
	lastRequestTime *int64

	chGetInstance chan muxerGetInstanceReq
}

func (m *muxer) initialize() {
	ctx, ctxCancel := context.WithCancel(m.parentCtx)
	m.ctx = ctx
	m.ctxCancel = ctxCancel
	m.created = time.Now()
	m.lastRequestTime = int64Ptr(time.Now().UnixNano())
	m.chGetInstance = make(chan muxerGetInstanceReq)

	m.Log(logger.Info, "created %s", func() string {
		if m.remoteAddr == "" {
			return "automatically"
		}
		return "(requested by " + m.remoteAddr + ")"
	}())

	m.wg.Add(1)
	go m.run()
}

func (m *muxer) Close() { m.ctxCancel() }

func (m *muxer) Log(level logger.Level, format string, args ...interface{}) {
	m.parent.Log(level, "[muxer %s] "+format, append([]interface{}{m.pathName}, args...)...)
}

func (m *muxer) PathName() string { return m.pathName }

func (m *muxer) run() {
	defer m.wg.Done()
	if err := m.runInner(); err != nil {
		m.Log(logger.Info, "destroyed: %v", err)
	}
	m.ctxCancel()
	m.parent.closeMuxer(m)
}

func (m *muxer) runInner() error {
	path, stream, err := m.pathManager.AddReader(defs.PathAddReaderReq{
		Author:        m,
		AccessRequest: defs.PathAccessRequest{Name: m.pathName, Query: m.query, SkipAuth: true},
	})
	if err != nil {
		return err
	}
	m.path = path
	defer m.path.RemoveReader(defs.PathRemoveReaderReq{Author: m})

	mi := &muxerInstance{pathName: m.pathName, stream: stream, parent: m}
	if err := mi.initialize(); err != nil {
		return err
	}
	defer mi.close()

	activityCheckTimer := time.NewTimer(closeCheckPeriod)
	for {
		select {
		case req := <-m.chGetInstance:
			req.res <- mi
		case <-activityCheckTimer.C:
			t := time.Unix(0, atomic.LoadInt64(m.lastRequestTime))
			if time.Since(t) >= time.Duration(m.closeAfter) {
				return fmt.Errorf("not used anymore")
			}
			activityCheckTimer = time.NewTimer(closeCheckPeriod)
		case <-m.ctx.Done():
			return fmt.Errorf("terminated")
		}
	}
}

func (m *muxer) getInstance() *muxerInstance {
	atomic.StoreInt64(m.lastRequestTime, time.Now().UnixNano())
	req := muxerGetInstanceReq{res: make(chan *muxerInstance)}
	select {
	case m.chGetInstance <- req:
		return <-req.res
	case <-m.ctx.Done():
		return nil
	}
}

// touchActivity updates lastRequestTime to keep the muxer alive.
func (m *muxer) touchActivity() {
	atomic.StoreInt64(m.lastRequestTime, time.Now().UnixNano())
}

func (m *muxer) APIReaderDescribe() defs.APIPathSourceOrReader {
	return defs.APIPathSourceOrReader{Type: "mseMuxer", ID: ""}
}

func int64Ptr(v int64) *int64 { return &v }
