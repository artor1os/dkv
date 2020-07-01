package replica

import (
	"crypto/rand"
	"math/big"
	"time"

	"github.com/artor1os/dkv/master"
	"github.com/artor1os/dkv/rpc"
	"github.com/artor1os/dkv/zookeeper"
)

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0]) // TODO: use hash
	}
	shard %= master.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Client struct {
	sm      *master.Client
	config  master.Config
	makeEnds func(zookeeper.Controller, int, int) []rpc.Endpoint
	zk zookeeper.Controller
	rid     int
	cid     int64
}

func NewClient(masters []rpc.Endpoint, makeEnds func(zookeeper.Controller, int, int) []rpc.Endpoint, zk zookeeper.Controller) *Client {
	c := new(Client)
	c.sm = master.NewClient(masters)
	c.makeEnds = makeEnds
	c.zk = zk
	c.cid = nrand()
	return c
}

func (c *Client) GetDelete(key string, op string) string {
	args := GetDeleteArgs{}
	args.CID = c.cid
	args.RID = c.rid
	c.rid++
	args.Key = key
	args.Op = op

	for {
		shard := key2shard(key)
		gid := c.config.Shards[shard]
		if peers, ok := c.config.Groups[gid]; ok {
			// try each server for the shard.
			servers := c.makeEnds(c.zk, gid, peers)
			for si := 0; si < len(servers); si++ {
				srv := servers[si]
				var reply GetDeleteReply
				ok := srv.Call("ShardKV.GetDelete", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		c.config = c.sm.Query(-1)
	}
}

func (c *Client) Get(key string) string {
	return c.GetDelete(key, "Get")
}

func (c *Client) Delete(key string) string {
	return c.GetDelete(key, "Remove")
}

func (c *Client) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.CID = c.cid
	args.RID = c.rid
	c.rid++
	args.Key = key
	args.Value = value
	args.Op = op

	for {
		shard := key2shard(key)
		gid := c.config.Shards[shard]
		if peers, ok := c.config.Groups[gid]; ok {
			servers := c.makeEnds(c.zk, gid, peers)
			for si := 0; si < len(servers); si++ {
				srv := servers[si]
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		c.config = c.sm.Query(-1)
	}
}

func (c *Client) Put(key string, value string) {
	c.PutAppend(key, value, "Put")
}
func (c *Client) Append(key string, value string) {
	c.PutAppend(key, value, "Append")
}

func (c *Client) Join(servers map[int]int) {
	c.sm.Join(servers)
}

func (c *Client) Leave(gids []int) {
	c.sm.Leave(gids)
}

func (c *Client) Query(num int) master.Config {
	return c.sm.Query(num)
}
