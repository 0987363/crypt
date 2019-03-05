package etcdv3

import (
	"context"
	"fmt"
	"time"

	"github.com/0987363/crypt/backend"

	goetcd "go.etcd.io/etcd/clientv3"
)

type Client struct {
	client    *goetcd.Client
	waitIndex uint64
}

var EtcdTimeout = 5 * time.Second

func New(machines []string) (*Client, error) {
	newClient, err := goetcd.New(goetcd.Config{
		Endpoints: machines,
	})
	if err != nil {
		return nil, fmt.Errorf("creating new etcdv3 client for crypt.backend.Client: %v", err)
	}
	return &Client{client: newClient, waitIndex: 0}, nil
}

func (c *Client) Get(key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), EtcdTimeout)
	defer cancel()

	return c.GetWithContext(ctx, key)
}

func (c *Client) GetWithContext(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) > 0 {
		return []byte(resp.Kvs[0].Value), nil
	}
	return nil, nil
}

func (c *Client) List(key string) (backend.KVPairs, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), EtcdTimeout)
	defer cancel()
	return c.ListWithContext(ctx, key)
}

func (c *Client) ListWithContext(ctx context.Context, key string) (backend.KVPairs, error) {
	resp, err := c.client.Get(ctx, key, goetcd.WithPrefix(), goetcd.WithSort(goetcd.SortByKey, goetcd.SortDescend))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	list := backend.KVPairs{}
	for _, ev := range resp.Kvs {
		list = append(list, &backend.KVPair{Key: string(ev.Key), Value: ev.Value})
	}
	return list, nil
}

func (c *Client) Set(key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.TODO(), EtcdTimeout)
	defer cancel()
	return c.SetWithContext(ctx, key, string(value))
}

func (c *Client) SetWithContext(ctx context.Context, key, value string) error {
	_, err := c.client.Put(ctx, key, value)
	return err
}

func (c *Client) Watch(key string, stop chan bool) <-chan *backend.Response {
	return c.WatchWithContext(context.Background(), key, stop)
}

func (c *Client) WatchWithContext(ctx context.Context, key string, stop chan bool) <-chan *backend.Response {
	respChan := make(chan *backend.Response, 0)
	go func() {
		ctx, cancel := context.WithCancel(ctx)
		go func() {
			<-stop
			cancel()
		}()

		rch := c.client.Watch(ctx, key, goetcd.WithPrefix())
		for wresp := range rch {
			for _, ev := range wresp.Events {
				respChan <- &backend.Response{[]byte(ev.Kv.Value), nil}
			}
		}
	}()
	return respChan
}
