package api_test

import (
	"context"
	"net"
	"net/http"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/test/mock"
)

// peerConn is Hertz's mock connection with a chosen remote address, so the
// trusted-proxy and loopback logic runs against a real peer instead of the
// 0.0.0.0:0 the plain test harness reports.
type peerConn struct {
	*mock.Conn
	addr net.Addr
}

func (p peerConn) RemoteAddr() net.Addr { return p.addr }

// as is a request from the given peer, driven through the whole middleware chain.
// ip is "10.1.2.3" or "[::1]"; host defaults to the first allowed host.
func as(h *server.Hertz, ip, method, path string, headers map[string]string) (int, http.Header, string) {
	ctx := app.NewContext(0)
	ctx.SetConn(peerConn{Conn: mock.NewConn(""), addr: &net.TCPAddr{IP: net.ParseIP(strings.Trim(ip, "[]")), Port: 40000}})
	ctx.Request.SetMethod(method)
	host := "api.ecoop.test"
	if v, ok := headers["Host"]; ok {
		host = v
	}
	ctx.Request.SetRequestURI("http://" + host + path)
	ctx.Request.Header.SetHost(host)
	for k, v := range headers {
		if k != "Host" {
			ctx.Request.Header.Set(k, v)
		}
	}
	h.ServeHTTP(context.Background(), ctx)
	out := http.Header{}
	ctx.Response.Header.VisitAll(func(k, v []byte) { out.Add(string(k), string(v)) })
	return ctx.Response.StatusCode(), out, string(ctx.Response.Body())
}
