package tagdb

import (
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/go-graphite/carbonapi/util"
)

type Http struct {
	proxy   *httputil.ReverseProxy
	limiter util.SimpleLimiter
}


type Config struct {
	Type string
	MaxConcurrentConnections int
	MaxTries                 int
	Timeout                  time.Duration
	KeepAliveInterval        time.Duration
	Url                      string
	User                     string
	Password                 string
	ForwardHeaders           bool
}

func NewHttp(cfg *Config) (*Http, error) {
	if cfg.Url == "" {
		// return error TODO
		return nil, nil
	}
	target, err := url.Parse(cfg.Url)
	if err != nil {
		return nil, err
	}
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

	return &Http{
		proxy:   proxy,
		limiter: util.NewSimpleLimiter(cfg.MaxConcurrentConnections),
	}, nil
}

func (h *Http) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.limiter.Enter()
	h.proxy.ServeHTTP(w, r)
	h.limiter.Leave()
}
