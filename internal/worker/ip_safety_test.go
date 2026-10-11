package worker

import (
	"net/netip"
	"testing"
)

func TestIsPublicDestinationIP(t *testing.T) {
	tests := []struct {
		name string
		ip   string
		want bool
	}{
		{name: "public IPv4", ip: "8.8.8.8", want: true},
		{name: "public IPv6", ip: "2606:4700:4700::1111", want: true},
		{name: "private IPv4", ip: "192.168.1.10", want: false},
		{name: "loopback IPv4", ip: "127.0.0.1", want: false},
		{name: "shared address space", ip: "100.64.0.1", want: false},
		{name: "documentation IPv4", ip: "192.0.2.1", want: false},
		{name: "benchmark IPv4", ip: "198.18.0.1", want: false},
		{name: "reserved IPv4", ip: "240.0.0.1", want: false},
		{name: "IPv6 unique local", ip: "fd00::1", want: false},
		{name: "IPv6 link local", ip: "fe80::1", want: false},
		{name: "IPv6 documentation", ip: "2001:db8::1", want: false},
		{name: "IPv6 multicast", ip: "ff02::1", want: false},
		{name: "unspecified IPv4", ip: "0.0.0.0", want: false},
		{name: "unspecified IPv6", ip: "::", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip, err := netip.ParseAddr(tt.ip)
			if err != nil {
				t.Fatalf("parse IP: %v", err)
			}

			if got := isPublicDestinationIP(ip); got != tt.want {
				t.Errorf("isPublicDestinationIP(%s) = %v, want %v", tt.ip, got, tt.want)
			}
		})
	}
}
