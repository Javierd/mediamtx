package mse

import (
	_ "embed"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/bluenviron/mediamtx/internal/auth"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/protocols/httpp"
	"github.com/bluenviron/mediamtx/internal/protocols/websocket"
)

//go:embed index.html
var indexHTML []byte

//go:embed player.js
var playerJS []byte

type httpServer struct {
	address        string
	encryption     bool
	serverKey      string
	serverCert     string
	allowOrigin    string
	trustedProxies conf.IPNetworks
	readTimeout    conf.Duration
	pathManager    serverPathManager
	parent         *Server

	inner *httpp.Server
}

func (s *httpServer) initialize() error {
	router := gin.New()
	router.SetTrustedProxies(s.trustedProxies.ToTrustedProxies()) //nolint:errcheck

	router.Use(s.middlewareOrigin)

	router.Use(s.onRequest)

	s.inner = &httpp.Server{
		Address:     s.address,
		ReadTimeout: time.Duration(s.readTimeout),
		Encryption:  s.encryption,
		ServerCert:  s.serverCert,
		ServerKey:   s.serverKey,
		Handler:     router,
		Parent:      s,
	}
	return s.inner.Initialize()
}

// Log implements logger.Writer.
func (s *httpServer) Log(level logger.Level, format string, args ...interface{}) {
	s.parent.Log(level, format, args...)
}

func (s *httpServer) close() { s.inner.Close() }

func (s *httpServer) middlewareOrigin(ctx *gin.Context) {
	ctx.Header("Access-Control-Allow-Origin", s.allowOrigin)
	ctx.Header("Access-Control-Allow-Credentials", "true")

	// preflight requests
	if ctx.Request.Method == http.MethodOptions &&
		ctx.Request.Header.Get("Access-Control-Request-Method") != "" {
		ctx.Header("Access-Control-Allow-Methods", "OPTIONS, GET")
		ctx.Header("Access-Control-Allow-Headers", "Authorization")
		ctx.AbortWithStatus(http.StatusNoContent)
		return
	}
}

func (s *httpServer) onRequest(ctx *gin.Context) {
	// WebSocket upgrade if requested
	upgrade := strings.ToLower(ctx.Request.Header.Get("Upgrade")) == "websocket"
	if upgrade && ctx.Request.Method == http.MethodGet {
		path := strings.TrimPrefix(ctx.Request.URL.Path, "/")
		if path == "" {
			ctx.Writer.WriteHeader(http.StatusNotFound)
			return
		}

		_, err := s.pathManager.FindPathConf(defs.PathFindPathConfReq{
			AccessRequest: defs.PathAccessRequest{
				Name:        path,
				Query:       ctx.Request.URL.RawQuery,
				Publish:     false,
				Proto:       auth.ProtocolMSE,
				Credentials: httpp.Credentials(ctx.Request),
				IP:          net.ParseIP(ctx.ClientIP()),
			},
		})
		if err != nil {
			var terr auth.Error
			if errors.As(err, &terr) {
				if terr.AskCredentials {
					ctx.Header("WWW-Authenticate", `Basic realm="mediamtx"`)
					ctx.Writer.WriteHeader(http.StatusUnauthorized)
					return
				}

				s.Log(logger.Info, "connection %v failed to authenticate: %v", httpp.RemoteAddr(ctx), terr.Message)
				<-time.After(auth.PauseAfterError)
				ctx.Writer.WriteHeader(http.StatusUnauthorized)
				return
			}

			ctx.Writer.WriteHeader(http.StatusNotFound)
			return
		}

		c, err := websocket.NewServerConn(ctx.Writer, ctx.Request)
		if err != nil {
			return
		}
		defer c.Close()

		mux, err := s.parent.getMuxer(serverGetMuxerReq{
			path:       path,
			remoteAddr: httpp.RemoteAddr(ctx),
			query:      ctx.Request.URL.RawQuery,
		})
		if err != nil {
			ctx.Writer.WriteHeader(http.StatusNotFound)
			return
		}

		mi := mux.getInstance()
		if mi == nil {
			ctx.Writer.WriteHeader(http.StatusNotFound)
			return
		}
		mi.handleWS(c)
		return
	}

	// static resources
	if ctx.Request.Method == http.MethodGet {
		switch {
		case strings.HasSuffix(ctx.Request.URL.Path, "/player.js"):
			ctx.Header("Cache-Control", "max-age=3600")
			ctx.Header("Content-Type", "application/javascript")
			ctx.Writer.WriteHeader(http.StatusOK)
			ctx.Writer.Write(playerJS)
			return
		case ctx.Request.URL.Path == "/favicon.ico":
			return
		default:
			ctx.Header("Cache-Control", "no-cache")
			ctx.Header("Content-Type", "text/html")
			ctx.Writer.WriteHeader(http.StatusOK)
			ctx.Writer.Write(indexHTML)
			return
		}
	}
}
