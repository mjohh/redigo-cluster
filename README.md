# redigo-cluster
it's may be the simplest cluster extension for redigo.
redigo-cluster has no redundant interfaces besides redis.Conn, which is as same as redigo.
sample: cluster_test.go
```go
func TestClusterPipelineCommands(t *testing.T) {
        //// 1) create a connection vi redis.DialTimeout offered by github.com/garyburd/redigo/redis
        //
	c, err := redis.DialTimeout("tcp", "172.23.25.178:6379", time.Second, time.Second, time.Second)
	if err != nil {
		t.Fatalf("error connection to database, %v", err)
	}

        //// 2) create cluster client vi the connection, which will build all connections to all nodes in cluster
        //
	cl, err := cluster.NewCluster(c)
	if err != nil {
		t.Fatalf("error when NewCluster, %v", err)
	}
	cluster.ClusterFlushDb(cl)

        //// 3) close all connections before exit
        //
	defer cl.Close()

        //// 4) sending cmd vi Send()
        //
	for _, cmd := range testCmds {
		if err := cl.Send(cmd.args[0].(string), cmd.args[1:]...); err != nil {
			t.Fatalf("Send(%v) returned error %v", cmd.args, err)
		}
	}
        //// 5) flush all commands, which will sharding to nodes according keys autoly
        //
	if err := cl.Flush(); err != nil {
		t.Errorf("Flush() returned error %v", err)
	}

        //// 6) Receive replys in sharding sequence, which is recorded in step 5)
        //
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
```
