package zookeeper

import (
	"strconv"
)

const (
	ElectionPath    = "/election"
	ISRPath         = "/isr"
	ConfigPath      = "/config"
	CommitIndexPath = "/commit"
	GroupPath = "/group"
	MasterPath = "/master"
)

func MakeGroupPath(root string, gid int) string {
	return root + "/" + strconv.Itoa(gid)
}
