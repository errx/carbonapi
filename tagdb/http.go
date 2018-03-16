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
	MaxConcurrentConnections int           `yaml:"maxConcurrentConnections"`
	Timeout                  time.Duration `yaml:"timeout"`
	KeepAliveInterval        time.Duration `yaml:"keepAliveInterval"`
	Url                      string        `yaml:"url"`
	User                     string        `yaml:"user"`
	Password                 string        `yaml:"password"`
	ForwardHeaders           bool          `yaml:"forwardHeaders"`
}

func NewHttpTagDb(cfg HttpConfig) *HttpDB {
	if cfg.Url == "" {
		return nil
	}
	target, err := url.Parse(cfg.Url)
	// TODO logging
	if err != nil {
		return nil
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

	return &HttpDB{
		proxy: proxy,
	}
}

func (h *HttpDB) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.proxy.ServeHTTP(w, r)
}
