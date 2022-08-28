package main

import (
	"context"
	"flag"
	"net"
	"os"

	"github.com/jackc/pgconn"
	log "github.com/sirupsen/logrus"
)

func main() {
	var listenAddress string
	flag.StringVar(&listenAddress, "listen", "127.0.0.1:15432", "Listen address")
	flag.Parse()

	ln, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatalln("serve at ip:", err)
	}
	for {
		client, err := ln.Accept()
		if err != nil {
			log.Fatalln("failed to accept client:", err)
		}
		log.Println("Accepted connection from", client.RemoteAddr())

		log.Println("Establishing new upstream connection", client.RemoteAddr())
		conn, err := pgconn.Connect(context.Background(), os.Getenv("PG_CONN"))
		if err != nil {
			log.Fatalln("failed to connect to PostgreSQL server:", err)
		}

		backend, err := NewPgPipeBackend(client, conn)
		if err != nil {
			log.Println(err)
			conn.Close(context.Background())
			continue
		}

		go func() {
			defer conn.Close(context.Background())
			err := backend.Run()
			if err != nil {
				log.Println(err)
			}
			log.Println("Closed connection from", client.RemoteAddr())
		}()
	}
}
