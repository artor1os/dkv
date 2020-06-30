package zookeeper

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type Controller interface {
	Sequence(string, []byte, int) error
	Index(string, int) ([]byte, error)
	Last(string) (int, error)
	First(string) (int, error)
	In(string, int) (bool, error)
	Add(string, int) error
	Delete(string, int) error
	ElectLeader(string, chan<- error)
	SetData(string, []byte) error
	Data(string) ([]byte, error)
	Register(string, int, int, string, func() error) error
	Find(string, int, int) (string, error)
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

func (c *cli) Sequence(path string, data []byte, index int) error {
	if err := c.CreateIfNotExist(path); err != nil {
		return err
	}
	path = path + "/guid_"
	result, err := c.conn.Create(path, data, zk.FlagSequence, defaultACL)
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

func (c *cli) Index(path string, index int) ([]byte, error) {
	path = path + "/guid_" + fmt.Sprintf("%010d", index)
	return c.Data(path)
}

var ErrNoChildren = fmt.Errorf("no children")
var ErrNodeNotExist = fmt.Errorf("node not exist")
var ErrNodeExist = fmt.Errorf("node exist")

func (c *cli) Last(path string) (int, error) {
	children, _, err := c.conn.Children(path)
	if err != nil {
		if errors.Is(err, zk.ErrNoNode) {
			return -1, ErrNodeNotExist
		}
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
		if errors.Is(err, zk.ErrNoNode) {
			return -1, ErrNodeNotExist
		}
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
	if err := c.CreateIfNotExist(path); err != nil && !errors.Is(err, zk.ErrNodeExists) {
		return err
	}
	path = path + "/" + strconv.Itoa(item)
	_, err := c.conn.Create(path, nil, 0, defaultACL)
	if errors.Is(err, zk.ErrNodeExists) {
		return ErrNodeExist
	}
	return nil
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
		if errors.Is(err, zk.ErrNoNode) {
			return "", ErrNodeNotExist
		}
		return "", err
	}
	if len(children) == 0 {
		return "", ErrNoChildren
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
	if err := c.CreateIfNotExist(path); err != nil && !errors.Is(err, zk.ErrNodeExists) {
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

func (c *cli) SetData(path string, data []byte) error {
	if err := c.CreateIfNotExist(path); err != nil && !errors.Is(err, zk.ErrNodeExists) {
		return err
	}
	exist, stat, err := c.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exist {
		return ErrNodeNotExist
	}
	_, err = c.conn.Set(path, data, stat.Version)
	return err
}

func (c *cli) Data(path string) ([]byte, error) {
	exist, _, err := c.conn.Exists(path)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, ErrNodeNotExist
	}
	b, _, err := c.conn.Get(path)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (c *cli) Register(path string, gid int, id int, addr string, initHook func() error) error {
	if err := c.CreateIfNotExist(path); err != nil && !errors.Is(err, zk.ErrNodeExists) {
		return fmt.Errorf("path: %v, err: %w", path, err)
	}
	shouldInit := false
	path = path + "/" + strconv.Itoa(gid)
	exist, _, err := c.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exist {
		_, err = c.conn.Create(path, nil, 0, defaultACL)
		if err != nil && !errors.Is(err, zk.ErrNodeExists) {
			return fmt.Errorf("path: %v, err: %w", path, err)
		}
		if err == nil {
			shouldInit = true
		}
	}
	// NOTE: maybe race

	children, _, err := c.conn.Children(path)
	if len(children) == 0 {
		shouldInit = true
	}

	path = path + "/" + strconv.Itoa(id)
	_, err = c.conn.Create(path, []byte(addr), zk.FlagEphemeral, defaultACL)
	if err != nil {
		return fmt.Errorf("path: %v, err: %w", path, err)
	}

	if shouldInit && initHook != nil {
		return initHook()
	}
	return nil
}

func (c *cli) Find(path string, gid int, id int) (string, error) {
	path = path + "/" + strconv.Itoa(gid) + "/" + strconv.Itoa(id)
	b, err := c.Data(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
