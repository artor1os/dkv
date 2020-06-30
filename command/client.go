package command

import (
	"net"
	"strconv"

	"github.com/artor1os/dkv/replica"
	"github.com/artor1os/dkv/rpc"
	"github.com/artor1os/dkv/util"
	"github.com/artor1os/dkv/zookeeper"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
)

var (
	c ClientOptions
)

type ClientOptions struct {
	port    *int
	ip      *string
	masters *int
	zk *string
}

func init() {
	cmdClient.Run = runClient
	c.port = cmdClient.Flag.Int("port", 8080, "http listen port")
	c.ip = cmdClient.Flag.String("ip", util.DetectedHostAddress(), "client <ip>|<server> address")
	c.masters = cmdClient.Flag.Int("masters", 3, "number of masters")
	c.zk = cmdClient.Flag.String("zk", "", "zookeeper servers")
}

var cmdClient = &Command{
	UsageLine: "client -port=8080",
	Short:     "start key-value client",
}

func runClient(cmd *Command, args []string) bool {
	startClient(c)
	return true
}

type joinArg map[int]int
type leaveArg []int

func startClient(options ClientOptions) {
	zk, err := zookeeper.New(util.ParsePeers(*options.zk))
	if err != nil {
		panic(err)
	}
	masters := rpc.MakeEndpoints(zk, zookeeper.MasterPath, 0, *options.masters)
	cli := replica.NewClient(masters, rpc.MakeEndpointsReplica, zk)
	r := gin.Default()

	r.GET("/get", func(c *gin.Context) {
		key := c.Query("key")
		value := cli.Get(key)
		c.JSON(200, value)
	})

	r.GET("/put", func(c *gin.Context) {
		key := c.Query("key")
		value := c.Query("value")
		cli.Put(key, value)
		c.JSON(200, "success")
	})

	r.GET("/append", func(c *gin.Context) {
		key := c.Query("key")
		value := c.Query("value")
		cli.Append(key, value)
		c.JSON(200, "success")
	})

	r.POST("/join", func(c *gin.Context) {
		arg := joinArg{}
		if err := c.MustBindWith(&arg, binding.JSON); err != nil {
			panic(err)
		}
		cli.Join(arg)
		c.JSON(200, "success")
	})

	r.POST("/leave", func(c *gin.Context) {
		arg := leaveArg{}
		if err := c.MustBindWith(&arg, binding.JSON); err != nil {
			panic(err)
		}
		cli.Leave(arg)
		c.JSON(200, "success")
	})

	r.GET("/query/:num", func(c *gin.Context) {
		num := c.Param("num")
		i, err := strconv.Atoi(num)
		if err != nil {
			_ = c.AbortWithError(400, err)
		}
		c.JSON(200, cli.Query(i))
	})

	r.Run(net.JoinHostPort(*options.ip, strconv.Itoa(*options.port)))
}
