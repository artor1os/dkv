package master

import (
	"crypto/rand"
	"math/big"
	"time"

	"github.com/artor1os/dkv/rpc"
)

type Client struct {
	servers []rpc.Endpoint
	leader  int
	rid     int
	cid     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func NewClient(servers []rpc.Endpoint) *Client {
	c := new(Client)
	c.servers = servers
	c.cid = nrand()
	return c
}

func (c *Client) Query(num int) Config {
	args := &QueryArgs{}
	args.Num = num
	args.RID = c.rid
	c.rid++
	args.CID = c.cid
	i := 0
	for {
		reply := QueryReply{}
		ok := c.servers[c.leader].Call("ShardMaster.Query", args, &reply)
		if ok && !reply.WrongLeader {
			return reply.Config
		}
		c.leader++
		c.leader %= len(c.servers)
		i++
		if i%len(c.servers) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (c *Client) Join(servers map[int]int) {
	args := &JoinArgs{}
	args.Servers = servers
	args.RID = c.rid
	c.rid++
	args.CID = c.cid
	i := 0
	for {
		reply := JoinReply{}
		ok := c.servers[c.leader].Call("ShardMaster.Join", args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		c.leader++
		c.leader %= len(c.servers)
		i++
		if i%len(c.servers) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (c *Client) Leave(gids []int) {
	args := &LeaveArgs{}
	args.GIDs = gids
	args.RID = c.rid
	c.rid++
	args.CID = c.cid
	i := 0
	for {
		reply := LeaveReply{}
		ok := c.servers[c.leader].Call("ShardMaster.Leave", args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		c.leader++
		c.leader %= len(c.servers)
		i++
		if i%len(c.servers) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (c *Client) Move(shard int, gid int) {
	args := &MoveArgs{}
	args.Shard = shard
	args.GID = gid
	args.RID = c.rid
	c.rid++
	args.CID = c.cid
	i := 0
	for {
		reply := MoveReply{}
		ok := c.servers[c.leader].Call("ShardMaster.Move", args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		c.leader++
		c.leader %= len(c.servers)
		i++
		if i%len(c.servers) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}
