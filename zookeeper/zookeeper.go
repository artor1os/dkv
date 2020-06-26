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
	First(string) (int, error)
	In(string, int) (bool, error)
	Add(string, int) error
	Delete(string, int) error
	ElectLeader(string, chan<- error)
	SetData(string, interface{}) error
	Data(string, interface{}) error
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
	return &cli{conn: conn}, nil
}

var defaultACL = zk.WorldACL(zk.PermAll)

func (c *cli) CreateIfNotExist(path string) error {
	exist, _, err := c.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exist {
		_, err = c.conn.Create(path, nil, 0, defaultACL)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *cli) Sequence(path string, data interface{}, index int) error {
	if err := c.CreateIfNotExist(path); err != nil {
		return err
	}
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	path = path + "/guid_"
	result, err := c.conn.Create(path, b, zk.FlagSequence, defaultACL)
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
	return c.Data(path, data)
}

var ErrNoChildren = fmt.Errorf("no children")
var ErrNodeNotExist = zk.ErrNoNode
var ErrNodeExist = zk.ErrNodeExists

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

func (c *cli) First(path string) (int, error) {
	children, _, err := c.conn.Children(path)
	if err != nil {
		return -1, err
	}
	if len(children) == 0 {
		return -1, ErrNoChildren
	}
	first := children[0]
	i, err := strconv.Atoi(strings.TrimPrefix(first, "guid_"))
	if err != nil {
		return -1, err
	}
	return i, nil
}

func (c *cli) In(path string, item int) (bool, error) {
	path = path + "/" + strconv.Itoa(item)
	exist, _, err := c.conn.Exists(path)
	return exist, err
}

func (c *cli) Add(path string, item int) error {
	if err := c.CreateIfNotExist(path); err != nil {
		return err
	}
	path = path + "/" + strconv.Itoa(item)
	_, err := c.conn.Create(path, nil, 0, defaultACL)
	return err
}

func (c *cli) Delete(path string, item int) error {
	path = path + "/" + strconv.Itoa(item)
	exist, stat, err := c.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exist {
		return ErrNodeNotExist
	}
	return c.conn.Delete(path, stat.Version)
}

func (c *cli) Prev(path string, node string) (string, error) {
	children, _, err := c.conn.Children(path)
	if err != nil {
		return "", err
	}
	if len(children) == 0 {
		return "", nil
	}
	prev := ""
	for _, c := range children {
		c = path + "/" + c
		if c == node {
			break
		}
		prev = c
	}
	return prev, nil
}

func (c *cli) ElectLeader(path string, elected chan<- error) {
	if err := c.CreateIfNotExist(path); err != nil {
		elected <- err
		return
	}
	nodePath := path + "/guid_"
	me, err := c.conn.CreateProtectedEphemeralSequential(nodePath, nil, defaultACL)
	if err != nil {
		elected <- err
		return
	}
	for {
		prev, err := c.Prev(path, me)
		if err != nil {
			elected <- err
			return
		}

		if prev == "" {
			elected <- nil
			return
		}

		exist, _, events, err := c.conn.ExistsW(prev)
		if err != nil {
			elected <- err
			return
		}
		if !exist {
			continue
		}

		for {
			event := <-events
			switch event.Type {
			case zk.EventNodeDeleted:
				break
			default:
				exist, _, events, err = c.conn.ExistsW(prev)
				if err != nil {
					elected <- err
					return
				}
				if !exist {
					break
				}
			}
		}
	}
}

func (c *cli) SetData(path string, data interface{}) error {
	if err := c.CreateIfNotExist(path); err != nil {
		return err
	}
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	exist, stat, err := c.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exist {
		return ErrNodeNotExist
	}
	_, err = c.conn.Set(path, b, stat.Version)
	return err
}

func (c *cli) Data(path string, data interface{}) error {
	exist, _, err := c.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exist {
		return ErrNodeNotExist
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
