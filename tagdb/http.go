package tagdb

import (
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

type HttpDB struct {
	proxy *httputil.ReverseProxy
}

type HttpConfig struct {
	MaxConcurrentConnections int
	MaxTries                 int
	Timeout                  time.Duration
	KeepAliveInterval        time.Duration
	Url                      string
	User                     string
	Password                 string
	ForwardHeaders           bool
}

func NewHttpTagDb(cfg HttpConfig) *HttpDB {
	target, _ := url.Parse(cfg.Url)
	proxy := httputil.NewSingleHostReverseProxy(target)

	proxy.Transport = &http.Transport{
		MaxIdleConnsPerHost: cfg.MaxConcurrentConnections,
		DialContext: (&net.Dialer{
			Timeout:   cfg.Timeout,
			KeepAlive: cfg.KeepAliveInterval,
			DualStack: true,
		}).DialContext,
	}

	origDirector := proxy.Director

	proxy.Director = func(req *http.Request) {
		origDirector(req)
		if !cfg.ForwardHeaders {
			req.Header = http.Header{}

		}
		if req.Header.Get("Authorization") == "" && cfg.User != "" && cfg.Password != "" {
			req.SetBasicAuth(cfg.User, cfg.Password)
		}
	}

	return &HttpDB{
		proxy: proxy,
	}
}

func (h *HttpDB) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.proxy.ServeHTTP(w, r)
}

