package server

import (
	"testing"
	_ "rpc"
	_ "net"
	"tribproto"
	"tribbleclient"
	"os"
	"runjob"
	"fmt"
	"rand"
	"time"
	"runtime"
)

const (
	START_PORT_NUMBER = 10000
)

func startServerGeneral(t *testing.T, port int, master_port int, lognum int, numservers int) *runjob.Job {
	// Semantics:  If master_port == port, then supply numservers param,
	// don't set -master.
	// Otherwise, omit -N and invoke with master:port
	runjob.Vlogf(2, "Starting server")
	logname := fmt.Sprintf("server.%d.log", lognum)
	logfile, err := os.OpenFile(logname, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("could not open log file")
	}
	portstr := fmt.Sprintf("-port=%d", port)
	var configstr string
	if port == master_port {
		configstr = fmt.Sprintf("-N=%d", numservers)
	} else {
		configstr = fmt.Sprintf("-master=localhost:%d", master_port)
	}
	server := runjob.NewJob("../bin/server", 0.0, logfile, portstr, configstr)
	if server == nil {
		t.Fatalf("Could not start server!")
	}
	return server
}

func startServer(t *testing.T, port int) *runjob.Job {
	server := startServerGeneral(t, port, port, 0, 1) 
	if (server != nil) {
		runjob.Delay(0.5)
		if (!server.Running()) {
			t.Fatalf("Could not start server!")
		}
	}
	return server
}

func startNServers(t *testing.T, startport int, numservers int) []*runjob.Job {
	ret := make([]*runjob.Job, numservers)
	ret[0] = startServerGeneral(t, startport, startport, 0, numservers)
	for i := 1; i < numservers; i++ {
		ret[i] = startServerGeneral(t, startport+i, startport, i, 0)
	}
	runjob.Delay(3)
	for i := 0; i < numservers; i++ {
		if !ret[i].Running() {
			t.Fatalf("could not start server ", i)
		}
	}
	return ret
}

func killServers(servers []*runjob.Job) {
	for _, server := range servers {
		if server != nil {
			runjob.Vlogf(2, "Killing server")
			server.Kill()
		}
	}
	for _, server := range servers {
		if (server != nil) {
			for (server.Running()) {
				runjob.Delay(0.1)
			}
		}
	}
}

func killServer(server *runjob.Job) {
	s := []*runjob.Job{server}
	killServers(s)
}

func randportno() int {
	rand.Seed(time.Nanoseconds())
	randsize := 60000 - START_PORT_NUMBER
	return (START_PORT_NUMBER + (rand.Int() % randsize))
}

func TestStartServer(t *testing.T) {
	port :=  randportno()
	server := startServer(t, port)
	killServer(server)
}

func dialServer(t *testing.T) (*runjob.Job, *tribbleclient.Tribbleclient, int) {
	port := randportno()
	server := startServer(t, port)
	tc := getClient(t, port)
	if (tc == nil) { // failure logged in getClient already
		killServer(server)
		return nil, nil, 0
	}
	return server, tc, port
}

func getClient(t *testing.T, port int) (*tribbleclient.Tribbleclient) {
	tc, err := tribbleclient.NewTribbleclient("localhost", fmt.Sprint(port))
	if (err != nil || tc == nil) {
		t.Fatalf("Could not dial server!")
		return nil
	}
	return tc
}

func TestDialServer(t *testing.T) {
	server, client, _ := dialServer(t)
	client.Close()
	killServer(server)
}

func TestCreateUser(t *testing.T) {
	server, client, _ := dialServer(t)
	defer killServer(server)
	defer client.Close()

	status, err := client.CreateUser("dga")
	if (status != tribproto.OK || err != nil) {
		t.Fatalf("Could not create user")
	}
}

func TestCreateTwice(t *testing.T) {
	server, client, _ := dialServer(t)
	defer killServer(server)
	defer client.Close()

	status, err := client.CreateUser("dga")
	if (status != tribproto.OK || err != nil) {
		t.Fatalf("Could not create user")
	}
	status, err = client.CreateUser("dga")
	if (status != tribproto.EEXISTS || err != nil) {
		t.Fatalf("Incorrect return code from trying to duplicate user")
	}
}

func signalDone(c *chan int) {
	if (c != nil) {
		*c <- 1
	}
}

func createSlam(t *testing.T, client *tribbleclient.Tribbleclient, c *chan int) {
	defer signalDone(c)
	for i := 0; i < 1000; i++ {
		status, err := client.CreateUser("dga")
		if (status != tribproto.EEXISTS || err != nil) {
			t.Fatalf("Incorrect return code from trying to duplicate user")
		}
	}
}

func createUsers(t *testing.T, c *tribbleclient.Tribbleclient, users []string) os.Error {
	for _, u := range users {
		status, err := c.CreateUser(u)
		if (status != tribproto.OK || err != nil) {
			t.Fatalf("Could not create user", u)
			return err
		}
	}
	return nil
}

func TestSubUnsubSlam(t *testing.T) {
	server, client, _ := dialServer(t)
	defer killServer(server)
	defer client.Close()

	users := []string{"dga", "bryant"}
	err := createUsers(t, client, users)
	if (err == nil) {
		subUnsubSlam(t, client, nil, "dga", "bryant")
	}
}
	

func TestCreateSlam(t *testing.T) {
	server, client, _ := dialServer(t)
	defer killServer(server)
	defer client.Close()

	status, err := client.CreateUser("dga")
	if (status != tribproto.OK || err != nil) {
		t.Fatalf("Could not create user")
	}
	createSlam(t, client, nil)
}

func TestConcurrentCreateSlam(t *testing.T) {
	oldprocs := runtime.GOMAXPROCS(-1)
	oldprocs = runtime.GOMAXPROCS(oldprocs+1)
	defer runtime.GOMAXPROCS(oldprocs)
	c := make(chan int)
	server, client, port := dialServer(t)
	defer killServer(server)
	defer client.Close()

	status, err := client.CreateUser("dga")
	if (status != tribproto.OK || err != nil) {
		t.Fatalf("Could not create user")
	}
	cli2 := getClient(t, port)
	if (cli2 == nil) {
		t.Fatalf("No client!  Eek!")
	}
	defer cli2.Close()
	go createSlam(t, client, &c)
	createSlam(t, cli2, nil)
	<- c
}

// returns old value of GOMAXPROCS
func add_N_procs(n int) int {
	curprocs := runtime.GOMAXPROCS(-1)
	return runtime.GOMAXPROCS(curprocs+n)
}

func TestEvil(t *testing.T) {
	defer runtime.GOMAXPROCS(add_N_procs(1))
	//c := make(chan int)
	server, cli1, port := dialServer(t)
	defer killServer(server)
	defer cli1.Close()

	cli2 := getClient(t, port)
	if (server == nil || cli1 == nil || cli2 == nil) {
		t.Fatalf("Setup failed")
		return
	}
	defer cli2.Close()

	subs, s, e := cli1.GetSubscriptions("dga")
	if (e != nil || s != tribproto.ENOSUCHUSER || len(subs) != 0) {
		t.Fatalf("non-user GetSubscriptions did not fail properly")
	}

	subto := []string{"bryant", "imoraru", "rstarzl"}
	cli1.CreateUser("dga")
	for _, sub := range subto {
		cli1.CreateUser(sub)
		cli2.AddSubscription("dga", sub)
	}
	subs, s, e = cli1.GetSubscriptions("dga")
	if (len(subs) != 3) {
		t.Fatalf("Subscription test failed")
	}
}

func subUnsubSlam(t *testing.T, client *tribbleclient.Tribbleclient, c *chan int, user string, target string) {
	defer signalDone(c)
	for i := 0; i < 300; i++ {
		status, err := client.AddSubscription(user, target)
		if (status != tribproto.OK || err != nil) {
			t.Fatalf("AddSubscription failed")
		}
		status, err = client.AddSubscription(user, target)
		if (status != tribproto.EEXISTS || err != nil) {
			t.Fatalf("AddSubscription did not fail when it should have")
		}
		status, err = client.RemoveSubscription(user, target)
		if (status != tribproto.OK || err != nil) {
			t.Fatalf("RemoveSubscription failed")
		}
	}
}

func checkTribblesSlam(t *testing.T, client *tribbleclient.Tribbleclient, c *chan int, user string, num_tribbles int) {
	defer signalDone(c)
	for query := 0; query < 6; query++ {
		tribbles, status, err := client.GetTribbles(user)
		if (err != nil || status != tribproto.OK) {
			t.Fatalf("Could not get tribbles for %s", user)
		}
		if (len(tribbles) != num_tribbles) {
			t.Fatalf("Expected %d tribbles, but got %d.  Tribble: %s ", num_tribbles, len(tribbles), tribbles)
		}
		for message := num_tribbles; message >= 1; message-- {
			expected := generic_post(user, message)
			// Remember, they show up in reverse order
			if (tribbles[num_tribbles-message].Contents != expected) {
				t.Fatalf("Did not get expected tribble.  Wanted %s, but tribble was %s.  Total tribble: %s", expected, tribbles[message-1], tribbles)
			}
		}
	}
}


func testNServers(t *testing.T, num_servers int) {
	portbase := randportno()
	servers := startNServers(t, portbase, num_servers)
	defer killServers(servers)
	clients := make([]*tribbleclient.Tribbleclient, num_servers)
	for i := 0; i < num_servers; i++ {
		clients[i] = getClient(t, portbase+i)
		if (clients[i] == nil) {
			t.Fatalf("Could not create client ", i)
		}
		defer clients[i].Close()
	}

	users := []string{"dga", "bryant"}
	err := createUsers(t, clients[0], users)
	if (err != nil) {
		t.Fatalf("Could not create users")
	}
	clients[0].AddSubscription("dga", "bryant")
	
	for i := 0; i < num_servers; i++ {
		subs, status, err := clients[i].GetSubscriptions("dga")
		if err != nil {
			t.Fatalf("testNservers (%d) failed on server %d:  got RPC error %s", num_servers, i, err)
		}
		if status != tribproto.OK {
			t.Fatalf("testNservers (%d) failed on server %d:  got trib error %d", num_servers, i, status)
		}
		if (len(subs) != 1 || subs[0] != "bryant") {
			t.Fatalf("testNServers (%d) failed on server %d: subs was %s", num_servers, i, subs)
		}
	}
}

func TestTwoServers(t *testing.T) {
	testNServers(t, 2)
}

func TestSixServers(t *testing.T) {
	testNServers(t, 6)
}

func generic_post(user string, postnum int) string {
	return fmt.Sprintf("this is post %d by %s", postnum, user)
}

func TestBigBad(t *testing.T) {
	num_servers := 4
	num_tribbles := 4
	portbase := randportno()
	servers := startNServers(t, portbase, num_servers)
	defer killServers(servers)
	clients := make([]*tribbleclient.Tribbleclient, num_servers)
	for i := 0; i < num_servers; i++ {
		clients[i] = getClient(t, portbase+i)
		if (clients[i] == nil) {
			t.Fatalf("Could not create client ", i)
		}
		defer clients[i].Close()
	}

	users := []string{"a", "b", "c", "d", "e"}
	err := createUsers(t, clients[0], users)
	if (err != nil) {
		t.Fatalf("Could not create users")
	}
	for _, user := range users {
		for i := 1; i <= num_tribbles; i++ {
			which_cli := rand.Int() % num_servers
			message := generic_post(user, i)
			status, err := clients[which_cli].PostTribble(user, message)
			if (err != nil || status != tribproto.OK) {
				t.Fatalf("Could not post tribble")
			}
		}
	}
	// Test every user from every server N=6 times to see how the system behaves
	// under load.
	for _, user := range users {
		for server := 0; server < num_servers; server++ {
			checkTribblesSlam(t, clients[server], nil, user, num_tribbles)
		}
	}

}

func TestBrokenCaching(t *testing.T) {
	num_servers := 2
	portbase := randportno()
	servers := startNServers(t, portbase, num_servers)
	defer killServers(servers)
	clients := make([]*tribbleclient.Tribbleclient, num_servers)
	for i := 0; i < num_servers; i++ {
		clients[i] = getClient(t, portbase+i)
		if (clients[i] == nil) {
			t.Fatalf("Could not create client ", i)
		}
		defer clients[i].Close()
	}
	user := "a"
	_ = createUsers(t, clients[0], []string{user})
	message := generic_post(user, 1)
	status, err := clients[0].PostTribble(user, message)
	if err != nil {
		t.Fatal("Got RPC error on PostTribble", err)
	}
	if status != tribproto.OK {
		t.Fatal("Got bad return code from PostTribble: ", status)
	}
	// Populate the cache regardless of the server it's on
	for server := 0; server < num_servers; server++ {
		checkTribblesSlam(t, clients[server], nil, user, 1)
	}
	message = generic_post(user, 2)
	_, _ = clients[0].PostTribble(user, message)
	// And now make sure we read the right result from BOTH of them.
	for server := 0; server < num_servers; server++ {
		checkTribblesSlam(t, clients[server], nil, user, 2)
	}
}