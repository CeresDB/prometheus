package ceresdb

import (
	"github.com/CeresDB/ceresdbproto/pkg/storagepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	storagepb.StorageServiceClient
	conn *grpc.ClientConn
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*1024)))
	if err != nil {
		return nil, err
	}

	c := storagepb.NewStorageServiceClient(conn)

	return &Client{
		StorageServiceClient: c,
		conn:                 conn,
	}, nil
}
