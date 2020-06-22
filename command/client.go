package command

import (
	"net"
	"strconv"

	"github.com/artor1os/dkv/replica"
	"github.com/artor1os/dkv/rpc"
	"github.com/artor1os/dkv/util"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
)

var (
	c ClientOptions
)

type ClientOptions struct {
	port    *int
	ip      *string
	masters *string
}

func init() {
	cmdClient.Run = runClient
	c.port = cmdClient.Flag.Int("port", 8080, "http listen port")
	c.ip = cmdClient.Flag.String("ip", util.DetectedHostAddress(), "client <ip>|<server> address")
	c.masters = cmdClient.Flag.String("masters", "", "all master nodes")
}

var cmdClient = &Command{
	UsageLine: "client -port=8080",
	Short:     "start key-value client",
}

func runClient(cmd *Command, args []string) bool {
	startClient(c)
	return true
}

type joinArg map[int][]string
type leaveArg []int

func startClient(options ClientOptions) {
	masters, err := rpc.MakeEndpoints(util.ParsePeers(*options.masters))
	if err != nil {
		panic(err)
	}
	cli := replica.NewClient(masters, rpc.MakeEndpoint)
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
