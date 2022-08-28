package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/dyv/pgpipe/pglogrepl"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type PgPipeBackend struct {
	client     *pgproto3.Backend
	clientConn net.Conn
	server     *pgconn.PgConn
	tlsConfig  *tls.Config
}

func NewPgPipeBackend(clientConn net.Conn, server *pgconn.PgConn) (*PgPipeBackend, error) {
	client := pgproto3.NewBackend(pgproto3.NewChunkReader(clientConn), clientConn)
	cer, err := tls.LoadX509KeyPair("./server.crt", "./server.key")
	if err != nil {
		return nil, errors.Wrap(err, "unable to load x509 pair")
	}

	return &PgPipeBackend{
		client:     client,
		clientConn: clientConn,
		server:     server,
		tlsConfig:  &tls.Config{Certificates: []tls.Certificate{cer}}}, nil
}

func (p *PgPipeBackend) Run() error {
	defer p.Close()

	err := p.handleStartup()
	if err != nil {
		return err
	}

	err = p.handleLogicalReplication()
	if err != nil {
		return err
	}
	return nil
}

func (p *PgPipeBackend) handleCopy(ctx context.Context, m *pgproto3.Query) error {
	query := m.Encode(nil)
	log.Printf("starting copy: %v", string(m.String))
	defer func() {
		log.Printf("copy complete:%v", string(m.String))

	}()
	err := p.server.SendBytes(ctx, query)
	if err != nil {
		return errors.Wrapf(err, "failed to send query")
	}

	copied := 0
	for {
		msg, err := p.server.ReceiveMessage(ctx)
		if err != nil {
			return errors.Wrapf(err, "failed to receive message")
		}
		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			copied += 1
			if copied%10000 == 0 {
				log.Println("example row: ", string(msg.Data))
			}
			err = p.client.Send(msg)
			if err != nil {
				return errors.Wrapf(err, "failed to send unhandled message type: %T", msg)
			}
		case *pgproto3.CopyOutResponse, *pgproto3.CopyDone:
			err := p.client.Send(msg)
			if err != nil {
				return errors.Wrapf(err, "failed to send copy out response: %T", msg)
			}

		case *pgproto3.CommandComplete:
			err := p.client.Send(msg)
			if err != nil {
				return errors.Wrapf(err, "failed to send copy out response: %T", msg)
			}
		case *pgproto3.ReadyForQuery:
			err := p.client.Send(msg)
			if err != nil {
				return errors.Wrapf(err, "failed to send copy out response: %T", msg)
			}
			return nil
		default:
			log.Printf("recieved control message: %T %v", msg, msg)
			panic("panic")
		}
	}
}

func (p *PgPipeBackend) handleReplication(ctx context.Context, m *pgproto3.Query) error {
	log.Println("starting replication: ", string(m.String))
	defer func() {
		log.Println("finished replication: ", string(m.String))
	}()
	query := m.Encode(nil)
	err := p.server.SendBytes(ctx, query)
	if err != nil {
		return errors.Wrapf(err, "failed to send query")
	}
	relations := map[uint32]*pglogrepl.RelationMessage{}
	connInfo := pgtype.NewConnInfo()
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			log.Debug("keepalive: attempting to receive client keep alive")
			resp, err := p.client.Receive()
			if err != nil {
				return errors.Errorf("commit err: %w", err)
			}
			switch resp := resp.(type) {
			case *pgproto3.CopyData:
				switch resp.Data[0] {
				case 'r':
					log.Debugf("keepalive: message resp: %T %v", resp, resp)
					commit := resp.Encode(nil)
					err = p.server.SendBytes(ctx, commit)
					if err != nil {
						return errors.Wrapf(err, "sending commit")
					}
					log.Debug("sent client heartbeat")
					nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
				}
			}
		}
		recvCtx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		msg, err := p.server.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return errors.Wrapf(err, "failed to receive message")
		}
		log.Printf("query response: %T", msg)
		switch msg := msg.(type) {
		case *pgproto3.CopyBothResponse:
			err := p.client.Send(msg)
			if err != nil {
				return errors.Wrapf(err, "failed to send copy out response: %T", msg)
			}
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
				}
				log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
				err = p.client.Send(msg)
				if err != nil {
					return errors.Errorf("failed to send keep alive message to client: %w", err)
				}
				if pkm.ReplyRequested {
					x, err := p.client.Receive()
					if err != nil {
						return errors.Errorf("failed to send keep alive message to client: %w", err)
					}
					nextStandbyMessageDeadline = time.Time{}
					log.Printf("keepalive client response: %T, %v", x, x)
				}

			case pglogrepl.XLogDataByteID:
				err := p.client.Send(msg)
				if err != nil {
					return errors.Errorf("failed to send xlog data: %w", err)
				}

				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParseXLogData failed:", err)
				}
				logicalMsg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					log.Fatalf("Parse logical replication message: %s", err)
				}
				log.Printf("Receive a logical replication message: %s", logicalMsg.Type())
				log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "LogicalMessage:", logicalMsg)
				switch logicalMsg := logicalMsg.(type) {
				case *pglogrepl.RelationMessage:
					relations[logicalMsg.RelationID] = logicalMsg

				case *pglogrepl.BeginMessage:
					resp, err := p.client.Receive()
					if err != nil {
						return errors.Errorf("commit err: %w", err)
					}
					log.Printf("begin message resp: %v", resp)
					commit := resp.Encode(nil)
					err = p.server.SendBytes(ctx, commit)
					if err != nil {
						return errors.Errorf("sending commit: %w", err)
					}

				case *pglogrepl.CommitMessage:
					resp, err := p.client.Receive()
					if err != nil {
						return errors.Errorf("commit err: %w", err)
					}
					log.Printf("commit message resp: %v", resp)
					commit := resp.Encode(nil)
					err = p.server.SendBytes(ctx, commit)
					if err != nil {
						return errors.Errorf("sending commit: %w", err)
					}

				case *pglogrepl.InsertMessage:
					rel, ok := relations[logicalMsg.RelationID]
					if !ok {
						log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
					}
					values := map[string]interface{}{}
					for idx, col := range logicalMsg.Tuple.Columns {
						colName := rel.Columns[idx].Name
						switch col.DataType {
						case 'n': // null
							values[colName] = nil
						case 'u': // unchanged toast
							// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
						case 't': //text
							val, err := decodeTextColumnData(connInfo, col.Data, rel.Columns[idx].DataType)
							if err != nil {
								log.Fatalf("error decoding column data: %w", err)
							}
							values[colName] = val
						}
					}
					log.Printf("INSERT INTO %s.%s: %v", rel.Namespace, rel.RelationName, values)

				case *pglogrepl.UpdateMessage:
					// ...
				case *pglogrepl.DeleteMessage:
					// ...
				case *pglogrepl.TruncateMessage:
					// ...

				case *pglogrepl.TypeMessage:
				case *pglogrepl.OriginMessage:
				default:
					log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
				}
			default:
				log.Printf("unhandled message: %T %v", msg, msg)
				panic("unhandled copy data")
			}
		case *pgproto3.CommandComplete:
			err := p.client.Send(msg)
			if err != nil {
				return errors.Wrapf(err, "failed to send copy out response: %T", msg)
			}
			return nil
		case *pgproto3.ReadyForQuery:
			err := p.client.Send(msg)
			if err != nil {
				return errors.Wrapf(err, "failed to send copy out response: %T", msg)
			}
			return nil
		default:
			panic("unhandled")
		}
	}

}

func (p *PgPipeBackend) proxyQuery(ctx context.Context, m *pgproto3.Query) error {
	log.Println("starting query: ", string(m.String))
	defer func() {
		log.Println("finished query: ", string(m.String))
	}()
	query := m.Encode(nil)
	err := p.server.SendBytes(ctx, query)
	if err != nil {
		return errors.Wrapf(err, "failed to send query")
	}

	for {
		msg, err := p.server.ReceiveMessage(ctx)
		if err != nil {
			return errors.Wrapf(err, "failed to receive message")
		}

		switch msg := msg.(type) {
		// TODO: We could intercept table descriptions to rewrite tables here
		case *pgproto3.RowDescription, *pgproto3.DataRow, *pgproto3.CommandComplete:
			err = p.client.Send(msg)
			if err != nil {
				return errors.Errorf("failed to send row descriptions: %w", err)
			}
		case *pgproto3.ReadyForQuery:
			err = p.client.Send(msg)
			if err != nil {
				return errors.Wrapf(err, "failed to send command complete")
			}
			return nil
		case *pgproto3.ErrorResponse:
			err = p.client.Send(msg)
			if err != nil {
				return errors.Wrapf(err, "failed to data row")
			}
			return pgconn.ErrorResponseToPgError(msg)
		default:
			return errors.Errorf("unexpected response: %T", msg)
		}
	}
}

func (p *PgPipeBackend) handleLogicalReplication() error {
	log.Println("handle logical replication")
	ctx := context.Background()

	for {
		log.Println("waiting for replication message")
		msg, err := p.client.Receive()
		if err != nil {
			return err
		}
		log.Printf("received message: %T %v", msg, msg)
		switch m := msg.(type) {
		case *pgproto3.Query:
			sql := m.String
			if strings.HasPrefix(sql, "SELECT DISTINCT t.schemaname, t.tablename") {
				err := p.proxyQuery(ctx, m)
				if err != nil {
					return err
				}
				continue
			}
			if strings.HasPrefix(sql, `CREATE_REPLICATION_SLOT`) {
				err := p.proxyQuery(ctx, m)
				if err != nil {
					return err
				}
				continue
			}
			if strings.HasPrefix(sql, `DROP_REPLICATION_SLOT`) {
				err := p.proxyQuery(ctx, m)
				if err != nil {
					return err
				}
				continue
			}

			if strings.HasPrefix(sql, `IDENTIFY_SYSTEM`) {
				err := p.proxyQuery(ctx, m)
				if err != nil {
					return err
				}
				continue
			}
			if strings.HasPrefix(sql, `START_REPLICATION`) {
				err := p.handleReplication(ctx, m)
				if err != nil {
					return err
				}
				continue
			}
			if strings.HasPrefix(sql, `BEGIN READ ONLY ISOLATION LEVEL`) {
				err := p.proxyQuery(ctx, m)
				if err != nil {
					return err
				}
				continue
			}
			if strings.HasPrefix(sql, `SELECT c.oid, c.relreplident  FROM pg_catalog.pg_class c`) {
				err := p.proxyQuery(ctx, m)
				if err != nil {
					return err
				}
				continue
			}
			if strings.HasPrefix(sql, `SELECT a.attname,       a.atttypid,       a.atttypmod`) {
				err := p.proxyQuery(ctx, m)
				if err != nil {
					return err
				}
				continue
			}
			if strings.HasPrefix(sql, `COPY`) {
				err := p.handleCopy(ctx, m)
				if err != nil {
					log.Println("handle copy error: ", err)
					return err
				}
				continue
			}
			if strings.HasPrefix(sql, `COMMIT`) {
				err := p.proxyQuery(ctx, m)
				if err != nil {
					return err
				}
				continue
			}
			log.Printf("unhandled query: %v", sql)
			return errors.New("unimplemented")
		case *pgproto3.Terminate:
			buf := m.Encode(nil)
			log.Printf("sending terminate: %v", m)
			err := p.server.SendBytes(ctx, buf)
			if err != nil {
				return errors.Errorf("failed to send terminate: %w", err)
			}
			return errors.New("terminate")
		default:
			log.Printf("unhandled message: %T %v", msg, msg)
			return errors.New("unimplemented")
		}
	}
}

func decodeTextColumnData(ci *pgtype.ConnInfo, data []byte, dataType uint32) (interface{}, error) {
	var decoder pgtype.TextDecoder
	if dt, ok := ci.DataTypeForOID(dataType); ok {
		decoder, ok = dt.Value.(pgtype.TextDecoder)
		if !ok {
			decoder = &pgtype.GenericText{}
		}
	} else {
		decoder = &pgtype.GenericText{}
	}
	if err := decoder.DecodeText(ci, data); err != nil {
		return nil, err
	}
	return decoder.(pgtype.Value).Get(), nil
}

func (p *PgPipeBackend) handleStartup() error {
	log.Println("handling startup")
	startupMessage, err := p.client.ReceiveStartupMessage()
	if err != nil {
		return errors.Wrapf(err, "error receiving startup message")
	}

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		p.handlePassword()
		err := p.client.Send(&pgproto3.AuthenticationOk{})
		if err != nil {
			return errors.Wrapf(err, "error sending ready for query")
		}
		err = p.client.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err != nil {
			return errors.Wrapf(err, "error sending ready for query")
		}
	case *pgproto3.SSLRequest:
		_, err = p.clientConn.Write([]byte{'S'})
		if err != nil {
			return errors.Wrapf(err, "error acking SSLRequest")
		}

		cfg := p.tlsConfig.Clone()
		p.clientConn = tls.Server(p.clientConn, cfg)
		p.client = pgproto3.NewBackend(pgproto3.NewChunkReader(p.clientConn), p.clientConn)
		return p.handleStartup()
	default:
		return fmt.Errorf("unknown startup message: %#v", startupMessage)
	}

	return nil
}

func (p *PgPipeBackend) handlePassword() error {
	salt := [4]byte{3, 3, 7, 9}
	err := p.client.Send(&pgproto3.AuthenticationMD5Password{Salt: salt})
	if err != nil {
		return errors.Wrap(err, "unable to request password")
	}
	msg, err := p.client.Receive()
	if err != nil {
		return errors.Wrap(err, "unable to recieve password message")
	}
	pswd, ok := msg.(*pgproto3.PasswordMessage)
	if !ok {
		return errors.Wrapf(err, "expected password message got different type: %T", pswd)
	}
	// TODO: Don't hard code the expected password
	if pswd.Password != "development" {
		// Logging auth tokens just for the tests debugging convenience...
		return errors.Wrap(err, "access denied")
	}
	return nil
}

func (p *PgPipeBackend) Close() error {
	return p.clientConn.Close()
}
