package harness

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"sync"
)

// The harness picks the host ports itself instead of letting the engine
// pick them. The engine picks a free port from the kernel's ephemeral range
// and binds it only later, so in between a parallel container, or any
// outgoing connection (whose source port comes from the same range), can
// take it. And `start` after a Kill binds the first run's ports again,
// which by then the engine may have handed to another container. So the
// harness picks ports outside the ephemeral range and holds each one, in
// this process, until its container is removed.

const (
	minHostPort    = 10000
	maxHostPort    = 65535
	reserveRetries = 1000
)

// ephemeralRangeFile holds the kernel's ephemeral port range.
const ephemeralRangeFile = "/proc/sys/net/ipv4/ip_local_port_range"

var ports struct {
	once   sync.Once
	lo, hi int // the ephemeral range, inclusive

	mu   sync.Mutex
	held map[int]bool
}

// ephemeralRange returns the kernel's ephemeral port range, or Linux's
// default if it can't be read.
func ephemeralRange() (lo, hi int) {
	ports.once.Do(func() {
		ports.lo, ports.hi = 32768, 60999
		b, err := os.ReadFile(ephemeralRangeFile)
		if err != nil {
			return
		}
		var l, h int
		if _, err := fmt.Sscan(string(b), &l, &h); err == nil && l <= h {
			ports.lo, ports.hi = l, h
		}
	})
	return ports.lo, ports.hi
}

// reservePorts fills dst with distinct host ports that are free on hostIP,
// outside the ephemeral range and not held for another container. They are
// held until releasePorts.
func reservePorts(dst []int) error {
	lo, hi := ephemeralRange()
	ports.mu.Lock()
	defer ports.mu.Unlock()
	if ports.held == nil {
		ports.held = make(map[int]bool)
	}
	for i := range dst {
		p, err := freePort(lo, hi)
		if err != nil {
			for _, q := range dst[:i] {
				delete(ports.held, q)
			}
			return err
		}
		ports.held[p] = true
		dst[i] = p
	}
	return nil
}

// freePort returns a random port outside [lo, hi] that isn't held and can
// be bound now. ports.mu must be held.
func freePort(lo, hi int) (int, error) {
	below := max(0, min(lo, maxHostPort+1)-minHostPort)
	above := max(0, maxHostPort-max(hi, minHostPort-1))
	if below+above == 0 {
		return 0, errors.New("no host ports outside the ephemeral range")
	}
	for range reserveRetries {
		p := minHostPort + rand.IntN(below+above)
		if p >= minHostPort+below {
			p = max(hi, minHostPort-1) + 1 + (p - minHostPort - below)
		}
		if ports.held[p] {
			continue
		}
		l, err := net.Listen("tcp", net.JoinHostPort(hostIP, strconv.Itoa(p)))
		if err != nil {
			continue
		}
		if err := l.Close(); err != nil {
			return 0, err
		}
		return p, nil
	}
	return 0, fmt.Errorf("no free host port found in %d tries", reserveRetries)
}

// releasePorts gives ports back once their container is removed.
func releasePorts(ps ...int) {
	ports.mu.Lock()
	defer ports.mu.Unlock()
	for _, p := range ps {
		delete(ports.held, p)
	}
}
