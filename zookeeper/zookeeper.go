package zookeeper

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type Controller interface {
	Sequence(string, interface{}, int) error
	Index(string, interface{}, int) error
	Last(string) (int, error)
}

type cli struct {
	conn *zk.Conn
}

func New(addrs []string) (*cli, error) {
	conn, session, err := zk.Connect(addrs, time.Second)
	if err != nil {
		return nil, err
	}
	for event := range session {
		if event.State == zk.StateConnected {
			break
		}
	}
	return &cli{conn:conn}, nil
}

func (c *cli) CreateIfNotExist(path string) {
	exist, _, _ := c.conn.Exists(path)
	if !exist {
		_, _ = c.conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
	}
}

func (c *cli) Sequence(path string, data interface{}, index int) error {
	c.CreateIfNotExist(path)
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	path = path + "/guid_"
	result, err := c.conn.Create(path, b, zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	i, err := strconv.Atoi(strings.TrimPrefix(result, path))
	if err != nil {
		return err
	}
	if i != index {
		exist, stat, err := c.conn.Exists(result)
		if err != nil {
			return err
		}
		if exist {
			if err := c.conn.Delete(result, stat.Version); err != nil {
				return err
			}
		}
		return fmt.Errorf("index not match, expected %v, actual %v", index, i)
	}
	return nil
}

func (c *cli) Index(path string, data interface{}, index int) error {
	path = path + "/guid_" + fmt.Sprintf("%010d", index)
	exist, _, err := c.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exist {
		return fmt.Errorf("not exist")
	}
	b, _, err := c.conn.Get(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, data); err != nil {
		return err
	}
	return nil
}

var ErrNoChildren = fmt.Errorf("no children")
var ErrNodeNotExist = zk.ErrNoNode

func (c *cli) Last(path string) (int, error) {
	children, _, err := c.conn.Children(path)
	if err != nil {
		return -1, err
	}
	if len(children) == 0 {
		return -1, ErrNoChildren
	}
	last := children[len(children)-1]
	i, err := strconv.Atoi(strings.TrimPrefix(last, "guid_"))
	if err != nil {
		return -1, err
	}
	return i, nil
}

