package worker

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"
)

func newSafeHTTPClient() *http.Client {
	allowedPrivateHosts := make(map[string]struct{})

	for _, host := range strings.Split(os.Getenv("WEBHOOK_ALLOWED_PRIVATE_HOSTS"), ",") {
		host = strings.ToLower(strings.TrimSpace(host))
		host = strings.TrimSuffix(host, ".")

		if host != "" {
			allowedPrivateHosts[host] = struct{}{}
		}
	}

	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		Proxy: nil,

		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, fmt.Errorf("parse destination address: %w", err)
			}

			portNumber, err := strconv.Atoi(port)
			if err != nil || portNumber < 1 || portNumber > 65535 {
				return nil, fmt.Errorf("invalid destination port")
			}

			host = strings.TrimSuffix(strings.ToLower(host), ".")
			if host == "" {
				return nil, fmt.Errorf("empty destination hostname")
			}

			_, privateHostAllowed := allowedPrivateHosts[host]

			var addresses []netip.Addr

			if ip, err := netip.ParseAddr(host); err == nil {
				addresses = []netip.Addr{ip}
			} else {
				resolved, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
				if err != nil {
					return nil, fmt.Errorf("resolve destination hostname: %w", err)
				}
				addresses = resolved
			}

			if len(addresses) == 0 {
				return nil, fmt.Errorf("destination hostname has no IP addresses")
			}

			checkedAddresses := make([]netip.Addr, 0, len(addresses))

			for _, ip := range addresses {
				ip = ip.Unmap()

				if !ip.IsValid() ||
					ip.IsUnspecified() ||
					ip.IsMulticast() ||
					ip.IsLinkLocalUnicast() ||
					ip.IsLinkLocalMulticast() {
					return nil, fmt.Errorf("destination resolves to a prohibited IP")
				}

				if privateHostAllowed {
				} else if ip.IsLoopback() || !isPublicDestinationIP(ip) {
					return nil, fmt.Errorf("destination resolves to a non-public IP")
				}

				checkedAddresses = append(checkedAddresses, ip)
			}

			var lastErr error

			for _, ip := range checkedAddresses {
				target := net.JoinHostPort(ip.String(), port)

				conn, err := dialer.DialContext(ctx, network, target)
				if err == nil {
					return conn, nil
				}

				lastErr = err
			}

			if lastErr == nil {
				return nil, fmt.Errorf("no destination IP could be connected")
			}

			return nil, fmt.Errorf("connect to destination: %w", lastErr)
		},
	}

	return &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,

		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
