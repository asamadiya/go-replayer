package main

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/peer"
)

type rawCodec struct{}

func (rawCodec) Marshal(v interface{}) ([]byte, error) { return v.([]byte), nil }
func (rawCodec) Unmarshal(data []byte, v interface{}) error {
	*(v.(*[]byte)) = data
	return nil
}
func (rawCodec) Name() string { return "proto" }

func init() { encoding.RegisterCodec(rawCodec{}) }

var peerCertLogOnce sync.Once

func main() {
	addr := flag.String("addr", ":28826", "listen address")
	cert := flag.String("cert", "", "server cert")
	key := flag.String("key", "", "server key")
	requireClientCert := flag.Bool("require-client-cert", false, "Require clients to present a TLS cert")
	flag.Parse()

	if *cert == "" || *key == "" {
		panic("--cert and --key are required")
	}

	serverCert, err := tls.LoadX509KeyPair(*cert, *key)
	if err != nil {
		panic(err)
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
	}
	if *requireClientCert {
		tlsCfg.ClientAuth = tls.RequireAnyClientCert
	}
	creds := credentials.NewTLS(tlsCfg)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		panic(err)
	}

	srv := grpc.NewServer(
		grpc.Creds(creds),
		grpc.ForceServerCodec(rawCodec{}),
		grpc.UnknownServiceHandler(func(_ interface{}, stream grpc.ServerStream) error {
			peerCertLogOnce.Do(func() {
				if p, ok := peer.FromContext(stream.Context()); ok {
					if tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(tlsInfo.State.PeerCertificates) > 0 {
						c := tlsInfo.State.PeerCertificates[0]
						fp := sha256.Sum256(c.Raw)
						fmt.Printf(
							"peer_cert subject=%q issuer=%q serial=%s sha256=%s\n",
							c.Subject.String(),
							c.Issuer.String(),
							c.SerialNumber.Text(16),
							hex.EncodeToString(fp[:]),
						)
						return
					}
				}
				fmt.Println("peer_cert not presented")
			})

			var req []byte
			if err := stream.RecvMsg(&req); err != nil {
				return err
			}
			return stream.SendMsg([]byte("ok"))
		}),
	)

	fmt.Printf("dummy gRPC target listening on %s\n", *addr)
	if err := srv.Serve(lis); err != nil {
		panic(err)
	}
}
