package server

import (
	"testing"
	_ "rpc"
	_ "net"
	"tribproto"
	"tribbleclient"
	"os"
	"../runjob/runjob"
	"log"
	"fmt"
	"rand"
	"time"
	"runtime"
)

type server struct {
	p *os.Process
	port int
}

type tribblerZoo struct {
	servers []server
}

func startOneServer(port int) (*os.Process, os.Error) {
	return os.StartProcess("bin/server", []string{"bin/server"},  nil)
}

func startServers(numServers int) *tribblerZoo {
	zoo := new(tribblerZoo)
	zoo.servers = make([]server, numServers)
	port := 8888;
	zoo.servers[0].port = port
	p, err := startOneServer(port)
	if (err != nil) {
		log.Printf("Could not start server")
		return nil
	}
	zoo.servers[0].p = p
	return zoo
}

func killServers(zoo *tribblerZoo) {
	for _, s := range zoo.servers {
		if (s.p != nil) {
			log.Printf("Killing %s\n", s)
			s.p.Kill()
		}
	}
}

func startServer(t *testing.T, port int) *runjob.Job {
	runjob.Vlogf(1, "Starting server")
	logfile, err := os.OpenFile("server.log", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if (err != nil) {
		log.Fatal("Could not open log file")
	}
	portstr := fmt.Sprint("-port=", port)
	server := runjob.NewJob("../bin/server", 0.0, logfile, portstr)
	if (server == nil) {
		t.Fatalf("Could not start server!")
	} else {
		runjob.Delay(0.5)
		if (!server.Running()) {
			t.Fatalf("Could not start server!")
		}
	}
	return server
}

func killServer(server *runjob.Job) {
	if (server != nil) {
		runjob.Vlogf(1, "Killing server")
		server.Kill()
		for (server.Running()) {
			runjob.Delay(0.1)
		}
	}
}
func randportno() int {
	rand.Seed(time.Nanoseconds())
	return (10000 + (rand.Int() % 50000))
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
	status, err := client.CreateUser("dga")
	if (status != tribproto.OK || err != nil) {
		t.Fatalf("Could not create user")
	}
	client.Close()
	killServer(server)
}

func TestCreateTwice(t *testing.T) {
	server, client, _ := dialServer(t)
	status, err := client.CreateUser("dga")
	if (status != tribproto.OK || err != nil) {
		t.Fatalf("Could not create user")
	}
	status, err = client.CreateUser("dga")
	if (status != tribproto.EEXISTS || err != nil) {
		t.Fatalf("Incorrect return code from trying to duplicate user")
	}
	client.Close()
	killServer(server)
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

func TestCreateSlam(t *testing.T) {
	server, client, _ := dialServer(t)
	status, err := client.CreateUser("dga")
	if (status != tribproto.OK || err != nil) {
		t.Fatalf("Could not create user")
	}
	createSlam(t, client, nil)
	client.Close()
	killServer(server)
}

func TestConcurrentCreateSlam(t *testing.T) {
	oldprocs := runtime.GOMAXPROCS(-1)
	oldprocs = runtime.GOMAXPROCS(oldprocs+1)
	defer runtime.GOMAXPROCS(oldprocs)
	c := make(chan int)
	server, client, port := dialServer(t)
	status, err := client.CreateUser("dga")
	if (status != tribproto.OK || err != nil) {
		t.Fatalf("Could not create user")
	}
	cli2 := getClient(t, port)
	if (cli2 == nil) {
		t.Fatalf("No client!  Eek!")
	}
	go createSlam(t, client, &c)
	createSlam(t, cli2, nil)
	<- c
	client.Close()
	cli2.Close()
	killServer(server)
}

func TestEvil(t *testing.T) {
	oldprocs := runtime.GOMAXPROCS(-1)
	oldprocs = runtime.GOMAXPROCS(oldprocs+1)
	defer runtime.GOMAXPROCS(oldprocs)
	//c := make(chan int)
	server, cli1, port := dialServer(t)
	cli2 := getClient(t, port)
	if (server == nil || cli1 == nil || cli2 == nil) {
		t.Fatalf("Setup failed")
		return
	}
	defer killServer(server)
	defer cli1.Close()
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
