package mse

import (
	"context"
	"fmt"
	"sync"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/stream"
)

type serverPathManager interface {
	FindPathConf(req defs.PathFindPathConfReq) (*conf.Path, error)
	AddReader(req defs.PathAddReaderReq) (defs.Path, *stream.Stream, error)
}

type serverParent interface {
	logger.Writer
}

// Server is an MSE server.
type Server struct {
	Address         string
	Encryption      bool
	ServerKey       string
	ServerCert      string
	AllowOrigin     string
	TrustedProxies  conf.IPNetworks
	ReadTimeout     conf.Duration
	MuxerCloseAfter conf.Duration
	PathManager     serverPathManager
	Parent          serverParent

	ctx       context.Context
	ctxCancel func()
	wg        sync.WaitGroup

	httpServer *httpServer
	muxers     map[string]*muxer

	// in
	chGetMuxer   chan serverGetMuxerReq
	chCloseMuxer chan *muxer
}

type serverGetMuxerRes struct {
	muxer *muxer
	err   error
}

type serverGetMuxerReq struct {
	path       string
	remoteAddr string
	query      string
	res        chan serverGetMuxerRes
}

// Initialize initializes the server.
func (s *Server) Initialize() error {
	ctx, ctxCancel := context.WithCancel(context.Background())

	s.ctx = ctx
	s.ctxCancel = ctxCancel
	s.muxers = make(map[string]*muxer)
	s.chGetMuxer = make(chan serverGetMuxerReq)
	s.chCloseMuxer = make(chan *muxer)

	s.httpServer = &httpServer{
		address:        s.Address,
		encryption:     s.Encryption,
		serverKey:      s.ServerKey,
		serverCert:     s.ServerCert,
		allowOrigin:    s.AllowOrigin,
		trustedProxies: s.TrustedProxies,
		readTimeout:    s.ReadTimeout,
		pathManager:    s.PathManager,
		parent:         s,
	}
	err := s.httpServer.initialize()
	if err != nil {
		ctxCancel()
		return err
	}

	s.Log(logger.Info, "listener opened on "+s.Address)

	s.wg.Add(1)
	go s.run()

	return nil
}

// Log implements logger.Writer.
func (s *Server) Log(level logger.Level, format string, args ...interface{}) {
	s.Parent.Log(level, "[MSE] "+format, args...)
}

// Close closes the server.
func (s *Server) Close() {
	s.Log(logger.Info, "listener is closing")
	s.ctxCancel()
	s.wg.Wait()
}

func (s *Server) run() {
	defer s.wg.Done()

outer:
	for {
		select {
		case req := <-s.chGetMuxer:
			mux, ok := s.muxers[req.path]
			if ok {
				req.res <- serverGetMuxerRes{muxer: mux}
				continue
			}
			req.res <- serverGetMuxerRes{muxer: s.createMuxer(req.path, req.remoteAddr, req.query)}

		case m := <-s.chCloseMuxer:
			if cur, ok := s.muxers[m.pathName]; ok && cur == m {
				delete(s.muxers, m.pathName)
			}

		case <-s.ctx.Done():
			break outer
		}
	}

	s.ctxCancel()
	s.httpServer.close()
}

func (s *Server) createMuxer(pathName string, remoteAddr string, query string) *muxer {
	m := &muxer{
		parentCtx:  s.ctx,
		remoteAddr: remoteAddr,
		wg:         &s.wg,
		pathName:   pathName,
		pathManager: s.PathManager,
		parent:     s,
		query:      query,
		closeAfter: s.MuxerCloseAfter,
	}
	m.initialize()
	s.muxers[pathName] = m
	return m
}

// closeMuxer is called by muxer.
func (s *Server) closeMuxer(m *muxer) {
	select {
	case s.chCloseMuxer <- m:
	case <-s.ctx.Done():
	}
}

func (s *Server) getMuxer(req serverGetMuxerReq) (*muxer, error) {
	req.res = make(chan serverGetMuxerRes)

	select {
	case s.chGetMuxer <- req:
		res := <-req.res
		return res.muxer, res.err
	case <-s.ctx.Done():
		return nil, fmt.Errorf("terminated")
	}
}