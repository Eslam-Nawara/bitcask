package respserver

import (
	"errors"

	"github.com/Eslam-Nawara/bitcask"
	"github.com/tidwall/resp"
)

var errInvalidArgsNum = errors.New("invalid number of arguments passed")

type RespServer struct {
	port         string
	server       *resp.Server
	bitcask      *bitcask.Bitcask
	dataStoreDir string
}

func New(dataStoreDir, port string) (*RespServer, error) {
	bitcask, err := bitcask.Open(dataStoreDir, bitcask.ReadWrite)
	if err != nil {
		return nil, err
	}

	return &RespServer{
		port:         port,
		server:       resp.NewServer(),
		bitcask:      bitcask,
		dataStoreDir: dataStoreDir,
	}, nil
}

func (server *RespServer) ListenAndServe() error {
	server.registerHandlers()
	err := server.server.ListenAndServe(server.port)
	if err != nil {
		return err
	}

	return nil
}

func (server *RespServer) Close() {
	server.bitcask.Close()
}

func (server *RespServer) registerHandlers() {
	server.server.HandleFunc("set", server.set)
	server.server.HandleFunc("get", server.get)
	server.server.HandleFunc("del", server.del)
}

func (server *RespServer) set(conn *resp.Conn, args []resp.Value) bool {
	if len(args) != 3 {
		conn.WriteError(errInvalidArgsNum)
	} else {
		err := server.bitcask.Put(args[1].String(), args[2].String())
		if err != nil {
			conn.WriteError(err)
		}
		conn.WriteSimpleString("OK")
	}

	return true
}

func (server *RespServer) get(conn *resp.Conn, args []resp.Value) bool {
	if len(args) != 2 {
		conn.WriteError(errInvalidArgsNum)
	} else {
		value, err := server.bitcask.Get(args[1].String())
		if err != nil {
			conn.WriteError(err)
		} else {
			conn.WriteString(value)
		}
	}

	return true
}

func (server *RespServer) del(conn *resp.Conn, args []resp.Value) bool {
	if len(args) != 2 {
		conn.WriteError(errInvalidArgsNum)
	} else {
		err := server.bitcask.Delete(args[1].String())
		if err != nil {
			conn.WriteError(err)
		} else {
			conn.WriteSimpleString("OK")
		}
	}

	return true
}
