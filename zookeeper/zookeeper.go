package zookeeper

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

type Controller interface {
	CreateIfNotExist(string, []byte, ...func() error) error
	Sequence(string, []byte, int) error
	Index(string, int) ([]byte, error)
	Last(string) (int, error)
	First(string) (int, error)
	In(string, int) (bool, error)
	All(string) ([]int, error)
	Add(string, int) error
	Remove(string, int) error
	Delete(string) error
	ElectLeader(string, int, chan<- error) (string, error)
	SetData(string, []byte) error
	Data(string) ([]byte, error)
	Register(string, int, int, string, ...func() error) error
	Find(string, int, int) (string, error)
}

type cli struct {
	conn *zk.Conn
}

func New(addrs []string) (*cli, error) {
	conn, session, err := zk.Connect(addrs, 10*time.Second)
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

func (c *cli) CreateIfNotExist(path string, data []byte, initHooks ...func() error) error {
	exist, _, err := c.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exist {
		_, err = c.conn.Create(path, data, 0, defaultACL)
		if err != nil && !errors.Is(err, zk.ErrNodeExists) {
			return err
		}
		if err == nil {
			var group errgroup.Group
			for _, h := range initHooks {
				group.Go(h)
			}
			return group.Wait()
		}
	}
	return nil
}

func (c *cli) Sequence(path string, data []byte, index int) error {
	if err := c.CreateIfNotExist(path, nil); err != nil {
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
	sort.Strings(children)
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
	sort.Strings(children)
	first := children[0]
	b, err := c.Data(path + "/" + first)
	if err != nil {
		return -1, fmt.Errorf("path: %v, err: %w", path+"/"+first, err)
	}
	id, err := strconv.Atoi(string(b))
	if err != nil {
		return -1, err
	}
	return id, nil
}

func (c *cli) In(path string, item int) (bool, error) {
	path = path + "/" + strconv.Itoa(item)
	exist, _, err := c.conn.Exists(path)
	return exist, err
}

func (c *cli) All(path string) ([]int, error) {
	children, _, err := c.conn.Children(path)
	if err != nil {
		return nil, err
	}
	ret := make([]int, len(children))
	for i, c := range children {
		id, err := strconv.Atoi(c)
		if err != nil {
			return nil, err
		}
		ret[i] = id
	}
	return ret, nil
}

func (c *cli) Add(path string, item int) error {
	if err := c.CreateIfNotExist(path, nil); err != nil && !errors.Is(err, zk.ErrNodeExists) {
		return err
	}
	path = path + "/" + strconv.Itoa(item)
	_, err := c.conn.Create(path, nil, 0, defaultACL)
	if errors.Is(err, zk.ErrNodeExists) {
		return ErrNodeExist
	}
	return nil
}

func (c *cli) Remove(path string, item int) error {
	path = path + "/" + strconv.Itoa(item)
	return c.Delete(path)
}

func (c *cli) Delete(path string) error {
	exist, stat, err := c.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exist {
		return ErrNodeNotExist
	}
	if err := c.conn.Delete(path, stat.Version); err != nil {
		if errors.Is(err, zk.ErrNoNode) {
			return ErrNodeNotExist
		}
		return err
	}
	return nil
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
	sort.Strings(children)
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

func (c *cli) ElectLeader(path string, id int, elected chan<- error) (string, error) {
	if err := c.CreateIfNotExist(path, nil); err != nil && !errors.Is(err, zk.ErrNodeExists) {
		return "", err
	}
	nodePath := path + "/guid_"
	me, err := c.conn.Create(nodePath, []byte(strconv.Itoa(id)), zk.FlagEphemeral|zk.FlagSequence, defaultACL)
	if err != nil {
		return "", err
	}
	go func() {
		for {
			prev, err := c.Prev(path, me)
			if err != nil {
				elected <- err
				return
			}

			logger := log.WithField("prev", prev).WithField("me", me)

			logger.Info("detect")

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
				logger.Info("watch: not exist")
				continue
			}
		Loop:
			for {
				event := <-events
				switch event.Type {
				case zk.EventNodeDeleted:
					logger.Info("node deleted")
					break Loop
				default:
					logger.Info("other event, re-watch")
					exist, _, events, err = c.conn.ExistsW(prev)
					if err != nil {
						elected <- err
						return
					}
					if !exist {
						logger.Info("re-watch: not exist")
						break Loop
					}
				}
			}
		}
	}()
	return me, nil
}

func (c *cli) SetData(path string, data []byte) error {
	if err := c.CreateIfNotExist(path, nil); err != nil && !errors.Is(err, zk.ErrNodeExists) {
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

func (c *cli) Register(path string, gid int, id int, addr string, initHooks ...func() error) error {
	if err := c.CreateIfNotExist(path, nil); err != nil && !errors.Is(err, zk.ErrNodeExists) {
		return fmt.Errorf("path: %v, err: %w", path, err)
	}
	path = path + "/" + strconv.Itoa(gid)
	if err := c.CreateIfNotExist(path, nil, initHooks...); err != nil {
		return err
	}

	path = path + "/" + strconv.Itoa(id)
	_, err := c.conn.Create(path, []byte(addr), zk.FlagEphemeral, defaultACL)
	if err != nil {
		return fmt.Errorf("path: %v, err: %w", path, err)
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
