package test_utils

import (
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TCPProxy is a proxy for TCP connections. It implements the Proxy interface to
// handle TCP traffic forwarding between the frontend and backend addresses.
type TCPProxy struct {
	t               testing.TB
	listener        net.Listener
	frontendAddr    *net.TCPAddr
	backendAddr     *net.TCPAddr
	MaxConnDuration time.Duration
	KilledCount     atomic.Int64
}

// NewTCPProxy creates a new TCPProxy.
func NewTCPProxy(listener net.Listener, backendAddr *net.TCPAddr, ops ...func(*TCPProxy)) (*TCPProxy, error) {
	// If the port in frontendAddr was 0 then ListenTCP will have a picked
	// a port to listen on, hence the call to Addr to get that actual port:
	proxy := &TCPProxy{
		listener:     listener,
		frontendAddr: listener.Addr().(*net.TCPAddr),
		backendAddr:  backendAddr,
	}

	for _, op := range ops {
		op(proxy)
	}

	return proxy, nil
}

func (proxy *TCPProxy) clientLoop(client *net.TCPConn, quit chan bool) {
	backend, err := net.DialTCP("tcp", nil, proxy.backendAddr)
	if err != nil {
		client.Close()
		return
	}

	var timer *time.Timer
	var timeoutChan <-chan time.Time
	if proxy.MaxConnDuration != 0 {
		timer = time.NewTimer(proxy.MaxConnDuration)
		timeoutChan = timer.C
	}

	event := make(chan int64)
	var broker = func(to, from *net.TCPConn) {
		written, err := io.Copy(to, from)
		if err != nil {
			// If the socket we are writing to is shutdown with
			// SHUT_WR, forward it to the other end of the pipe:
			if err, ok := err.(*net.OpError); ok && err.Err == syscall.EPIPE {
				from.CloseWrite()
			}
		}
		to.CloseRead()
		event <- written
	}

	go broker(client, backend)
	go broker(backend, client)

	var transferred int64
	for i := 0; i < 2; i++ {
		select {
		case written := <-event:
			transferred += written
		case <-timeoutChan:
			// Interrupt the two brokers and "join" them.
			client.Close()
			backend.Close()
			for ; i < 2; i++ {
				transferred += <-event
			}
			proxy.t.Log(fmt.Sprintf("TCPProxy: killed connection from %s to %s due to timeout", client.RemoteAddr(), proxy.backendAddr))
			proxy.KilledCount.Add(1)
			return
		case <-quit:
			if timer != nil {
				timer.Stop()
			}
			// Interrupt the two brokers and "join" them.
			client.Close()
			backend.Close()
			for ; i < 2; i++ {
				transferred += <-event
			}
			return
		}
	}
	if timer != nil {
		timer.Stop()
	}
	client.Close()
	backend.Close()
}

// Run starts forwarding the traffic using TCP.
func (proxy *TCPProxy) Run() {
	quit := make(chan bool)
	defer close(quit)
	for {
		client, err := proxy.listener.Accept()
		if err != nil {
			return
		}
		go proxy.clientLoop(client.(*net.TCPConn), quit)
	}
}

// Close stops forwarding the traffic.
func (proxy *TCPProxy) Close() { proxy.listener.Close() }

// FrontendAddr returns the TCP address on which the proxy is listening.
func (proxy *TCPProxy) FrontendAddr() net.Addr { return proxy.frontendAddr }

// BackendAddr returns the TCP proxied address.
func (proxy *TCPProxy) BackendAddr() net.Addr { return proxy.backendAddr }

func WithMaxConnDuration(duration time.Duration) func(*TCPProxy) {
	return func(proxy *TCPProxy) {
		proxy.MaxConnDuration = duration
	}
}

func WithTCPProxyLogger(t testing.TB) func(*TCPProxy) {
	return func(proxy *TCPProxy) {
		proxy.t = t
	}
}
