package command

import (
	"github.com/artor1os/dkv/replica"
	"github.com/artor1os/dkv/rpc"
	"github.com/artor1os/dkv/util"
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

func startClient(options ClientOptions) {
	masters, err := rpc.MakeEndpoints(util.ParsePeers(*c.masters))
	if err != nil {
		panic(err)
	}
	replica.NewClient(masters, rpc.MakeEndpoint)
}
