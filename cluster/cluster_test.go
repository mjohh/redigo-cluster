package cluster_test

import (
	"github.com/garyburd/redigo/redis"
	"github.com/mjohh/redigo-cluster/cluster"
	"reflect"
	"testing"
	"time"
	//	"fmt"
)

var testCmds = []struct {
	args     []interface{}
	expected interface{}
}{
	{
		[]interface{}{"PING"},
		"PONG",
	},
	{
		[]interface{}{"SET", "foo", "bar"},
		"OK",
	},
	{
		[]interface{}{"GET", "foo"},
		[]byte("bar"),
	},
	{
		[]interface{}{"GET", "nokey"},
		nil,
	},
	{
		[]interface{}{"DEL", "nokey"},
		int64(0),
	},
	//{
	//[]interface{}{"MGET", "nokey", "foo"},
	//[]interface{}{nil, []byte("bar")},
	//},
	{
		[]interface{}{"INCR", "mycounter"},
		int64(1),
	},
	{
		[]interface{}{"LPUSH", "mylist", "foo"},
		int64(1),
	},
	{
		[]interface{}{"LPUSH", "mylist", "bar"},
		int64(2),
	},
	{
		[]interface{}{"LRANGE", "mylist", 0, -1},
		[]interface{}{[]byte("bar"), []byte("foo")},
	},
	/*
	   {
	           []interface{}{"MULTI"},
	           "OK",
	   },
	   {
	           []interface{}{"LRANGE", "mylist", 0, -1},
	           "QUEUED",
	   },
	   {
	           []interface{}{"PING"},
	           "QUEUED",
	   },
	   {
	           []interface{}{"EXEC"},
	           []interface{}{
	                   []interface{}{[]byte("bar"), []byte("foo")},
	                   "PONG",
	           },
	   },
	*/
}

func TestClusterCommands(t *testing.T) {
	c, err := redis.DialTimeout("tcp", "172.23.25.178:6379", time.Second, time.Second, time.Second)
	if err != nil {
		t.Fatalf("error connection to database, %v", err)
	}

	cl, err := cluster.NewCluster(c)
	if err != nil {
		t.Fatalf("error when NewCluster, %v", err)
	}
	//redis.ClusterPrint(cluster)
	cluster.ClusterFlushDb(cl)

	defer cl.Close()

	for _, cmd := range testCmds {
		actual, err := cl.Do(cmd.args[0].(string), cmd.args[1:]...)
		if err != nil {
			t.Errorf("Do(%v) returned error %v", cmd.args, err)
			continue
		}
		if !reflect.DeepEqual(actual, cmd.expected) {
			t.Errorf("Do(%v) = %v, want %v", cmd.args, actual, cmd.expected)
		}
	}
}

func TestClusterPipelineCommands(t *testing.T) {
	c, err := redis.DialTimeout("tcp", "172.23.25.178:6379", time.Second, time.Second, time.Second)
	if err != nil {
		t.Fatalf("error connection to database, %v", err)
	}
	cl, err := cluster.NewCluster(c)
	if err != nil {
		t.Fatalf("error when NewCluster, %v", err)
	}
	//redis.ClusterPrint(cluster)
	cluster.ClusterFlushDb(cl)

	defer cl.Close()

	for _, cmd := range testCmds {
		if err := cl.Send(cmd.args[0].(string), cmd.args[1:]...); err != nil {
			t.Fatalf("Send(%v) returned error %v", cmd.args, err)
		}
	}
	if err := cl.Flush(); err != nil {
		t.Errorf("Flush() returned error %v", err)
	}

	for _, cmd := range testCmds {
		actual, err := cl.Receive()
		if err != nil {
			t.Fatalf("Receive(%v) returned error %v", cmd.args, err)
		}
		if !reflect.DeepEqual(actual, cmd.expected) {
			t.Errorf("Recv(%v) = %v, want %v", cmd.args, actual, cmd.expected)
		}
	}
}
