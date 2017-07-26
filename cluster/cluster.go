// Copyright 2017 Mjohh@163.ocm
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

/*
/* compare to redis-go-cluster, i think there are some worth to be improved:
/* 1) Just implement redis.Conn interface: unnecessary define new interface.
/* 2) Loading cluster info dynamicly: not cfg staticly  when creation.
/* 3) More simple pipeline implementation:only store reply pos sequence.
/* 4) scan func is too complicated, though it is used easily.
*/

package cluster

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strings"
	"sync"
	"time"
)

type slot struct {
	redis.Conn
	startslot uint16
	endslot   uint16
	ip        string
	port      int64
}

type cluster struct {
	slots []*slot
	// The sequence of slot idx.
	// Cmds in one pipeline operation,
	// will be sharding into low level pipelines according key values.
	// Useful for getting replys in order from low level pipelines.
	shardingSeq []int
	// If slots[i] has been sharding, shardings[i] will be true.
	// Useful for flush op.
	shardings []bool
}

const (
	kClusterSlots = 16384
)

// NewCluster return a cluster obj contain all connections to each node in cluster.
func NewCluster(c redis.Conn) (redis.Conn, error) {
	rv, err := c.Do("info")
	if err != nil {
		return nil, err
	}
	s, e := redis.String(rv, err)
	if e != nil {
		return nil, e
	}
	if !strings.Contains(s, "cluster_enabled:1") {
		return nil, errors.New("redigo-cluster: non-cluster!")
	}
	slots, err := FetchSlots(c)
	if err != nil {
		return nil, err
	}

	err = connectAll(slots)
	if err != nil {
		// close left connections firstly
		closeAll(slots)
		// try again
		slots, err = reloadSlots(slots)
		if err != nil {
			return nil, err
		}
		err = connectAll(slots)
		if err != nil {
			return nil, err
		}
	}
	shardings := make([]bool, len(slots))
	return &cluster{slots: slots, shardingSeq: make([]int, 0), shardings: shardings}, nil
}

// findSlot return slot index via key hash val
func (cl *cluster) findSlot(key string) (int, error) {
	hashslot := hash(key)
	for i, slot := range cl.slots {
		if slot.startslot < hashslot && hashslot <= slot.endslot {
			return i, nil
		}
	}
	return -1, errors.New("redigo-cluster: could not find slot via hash!")
}

// implement Conn interface
func (cl *cluster) Close() error {
	closeAll(cl.slots)
	return nil
}

// Err returns a non-nil value when the connection is not usable
func (cl *cluster) Err() error {
	return nil
}

// Do Sends a command to the server and returns the received reply.
// if no key, chose slot[0]
func (cl *cluster) Do(cmd string, args ...interface{}) (reply interface{}, err error) {
	if len(args) == 0 {
		reply, err = cl.slots[0].Do(cmd)
		return
	}
	key, ok := args[0].(string)
	if !ok {
		return nil, errors.New("redigo-cluster: args[0] is not string!")
	}
	i, e := cl.findSlot(key)
	if e != nil {
		return nil, e
	}
	reply, err = cl.slots[i].Do(cmd, args...)
	return
}

// Send writes the command to the client's output buffer. And recording
// sharding sequence and sharding slot, which are useful for Receive() and Flush()
func (cl *cluster) Send(cmd string, args ...interface{}) error {
	if len(args) == 0 {
		err := cl.slots[0].Send(cmd)
		if err == nil {
			cl.shardingSeq = append(cl.shardingSeq, 0)
			cl.shardings[0] = true
		}
		return err
	}
	key, ok := args[0].(string)
	if !ok {
		return errors.New("redigo-cluster: args[0] is not string!")
	}
	i, e := cl.findSlot(key)
	if e != nil {
		return e
	}
	err := cl.slots[i].Send(cmd, args...)
	if err == nil {
		cl.shardingSeq = append(cl.shardingSeq, i)
		cl.shardings[i] = true
	}
	return err
}

// Flush flushes the output buffer to the Redis server.
// Each pipelined conn will be fulshed on a goroutine, and
// the func will be broken until all goroutines return.
// The last error of all goroutine will be return as func error.
func (cl *cluster) Flush() error {
	var wg sync.WaitGroup
	var err error
	for i, b := range cl.shardings {
		if b == false {
			continue
		}
		// clean flag
		cl.shardings[i] = false
		wg.Add(1)
		f := func(i int) {
			e := cl.slots[i].Flush()
			if e != nil {
				err = e
			}
			wg.Done()
		}
		go f(i)
	}
	wg.Wait()
	return err
}

/*
func (cl *cluster) Flush() error {
	var err error
	for i, b := range cl.shardings {
		if b==false{
			continue
		}
		// clean flag
		cl.shardings[i] = false
		e := cl.slots[i].conn.Flush()
		if e != nil {
			err = e
		}
	}
	return err
}
*/
// Receive receives a single reply from the Redis server.
func (cl *cluster) Receive() (reply interface{}, err error) {
	if len(cl.shardingSeq) > 0 {
		idx := cl.shardingSeq[0]
		reply, err = cl.slots[idx].Receive()
		cl.shardingSeq = cl.shardingSeq[1:]
		return
	}
	reply, err = nil, errors.New("redigo-cluster: no reply!")
	return
}

// FetchSlots return slot slice, by Sending "cluster slots" cmd. Attention that
// only master ip port info is used.
func FetchSlots(c redis.Conn) ([]*slot, error) {
	// fetch slots info
	reply, err := c.Do("cluster", "slots")
	if err != nil {
		return nil, err
	}

	r, ok := reply.([]interface{})
	if !ok {
		return nil, errors.New("redigo-cluster: reply is not type of []interface{}")
	}
	slots := make([]*slot, 0)
	for _, sl := range r {
		slt, ok := sl.([]interface{})
		if !ok {
			return nil, errors.New("redigo-cluster: slot is not type of []interface{}")
		}
		/// start, end slots
		start, err := redis.Int64(slt[0], nil)
		if err != nil {
			return nil, err
		}
		end, err := redis.Int64(slt[1], nil)
		if err != nil {
			return nil, err
		}
		/// master ip, port info
		master, ok := slt[2].([]interface{})
		if !ok {
			return nil, errors.New("redigo-cluster: slot[2] is not type of []interface{}")
		}
		ip, err := redis.String(master[0], nil)
		if err != nil {
			return nil, err
		}
		port, err := redis.Int64(master[1], nil)
		if err != nil {
			return nil, err
		}
		// id
		_, err = redis.String(master[2], nil)
		if err != nil {
			return nil, err
		}
		o := slot{Conn: nil, startslot: uint16(start), endslot: uint16(end), ip: ip, port: port}
		slots = append(slots, &o)
	}
	return slots, nil
}

// closeAll close all connections. This is called when connectAll() fail
func closeAll(slots []*slot) {
	for _, slot := range slots {
		if slot.Conn == nil {
			continue
		}
		slot.Close()
	}
}

// connectAll try to build all connections to all master slots(nodes) in cluster
func connectAll(slots []*slot) error {
	for _, slot := range slots {
		s := fmt.Sprintf("%s:%d", slot.ip, slot.port)
		c, e := redis.DialTimeout("tcp", s, time.Second, time.Second, time.Second)
		if c == nil || e != nil {
			return errors.New("redigo-cluster: connect all slots fail!")
		}
		slot.Conn = c
	}
	return nil
}

// reloadSlots return new cluster slot objs, via any one existing valid connection.
// it's always called when one connection faild, which may cause cluster info change.
func reloadSlots(slots []*slot) ([]*slot, error) {
	for _, slot := range slots {
		s := fmt.Sprintf("%s:%d", slot.ip, slot.port)
		c, e := redis.DialTimeout("tcp", s, time.Second, time.Second, time.Second)
		if c != nil && e == nil {
			newslots, e := FetchSlots(c)
			if e == nil {
				return newslots, nil
			}
		}
	}
	return nil, errors.New("redigo-cluster: reload slots fail!")
}

// caculate hash according string within {}, otherwise whole key will be used.
func hash(key string) uint16 {
	var s, e int
	for s = 0; s < len(key); s++ {
		if key[s] == '{' {
			break
		}
	}
	if s == len(key) {
		return crc16(key) & (kClusterSlots - 1)
	}

	for e = s + 1; e < len(key); e++ {
		if key[e] == '}' {
			break
		}
	}

	if e == len(key) || e == s+1 {
		return crc16(key) & (kClusterSlots - 1)
	}

	return crc16(key[s+1:e]) & (kClusterSlots - 1)
}

func ClusterFlushDb(c redis.Conn) error {
	cl, ok := c.(*cluster)
	if !ok {
		return fmt.Errorf("redigo-cluster: the c in ClusterFlushDb(c) is not *cluster type, but %T", c)
	}
	for _, slot := range cl.slots {
		slot.Do("FLUSHDB")
	}
	return nil
}

func ClusterPrint(c redis.Conn) error {
	cl, ok := c.(*cluster)
	if !ok {
		return fmt.Errorf("redigo-cluster: the c in ClusterFlushDb(c) is not *cluster type, but %T", c)
	}
	for _, slot := range cl.slots {
		fmt.Println(slot)
	}
	return nil
}
