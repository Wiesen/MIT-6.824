// A view consists of a view number and the identity (network port name) of the view's primary and backup servers
// The primary in a view must always be either the primary or the backup of the previous view.
//* An exception: when the viewservice first starts, it should accept any server at all as the first primary.
//* The backup in a view can be any server (other than the primary), or can be altogether missing if no server is available (represented by an empty string, "")

//* Each key/value server should send a Ping RPC once per PingInterval (see viewservice/common.go)
//* The view service replies to the Ping with a description of the current view
// If the viewservice doesn't receive a Ping from a server for DeadPings PingIntervals, the viewservice should consider the server to be dead
// When a server re-starts after a crash, it should send one or more Pings with an argument of zero to inform the view service that it crashed

//* The view service proceeds to a new view if it hasn't received recent Pings from both primary and backup,
// or if the primary or backup crashed and restarted,
//* or if there is no backup and there is an idle server (a server that's been Pinging but is neither the primary nor the backup).

//* But the view service must not change views (i.e., return a different view to callers)
//* until the primary from the current view acknowledges that it is operating in the current view (by sending a Ping with the current view number)
//* If the view service has not yet received an acknowledgment for the current view from the primary of the current view,
//* the view service should not change views even if it thinks that the primary or backup has died.
//* The acknowledgment rule prevents the view service from getting more than one view ahead of the key/value servers.

//! Hint: you'll want to add field(s) to ViewServer in server.go in order to keep track of the most recent time at which the viewservice has heard a Ping from each server. Perhaps a map from server names to time.Time. You can find the current time with time.Now()
//! Hint: add field(s) to ViewServer to keep track of the current view.

//! Hint: you'll need to keep track of whether the primary for the current view has acknowledged it (in PingArgs.Viewnum).

//! Hint: your viewservice needs to make periodic decisions, for example to promote the backup if the viewservice has missed DeadPings pings from the primary. Add this code to the tick() function, which is called once per PingInterval.
// Hint: there may be more than two servers sending Pings. The extra ones (beyond primary and backup) are volunteering to be backup if needed.
// Hint: the viewservice needs a way to detect that a primary or backup has failed and re-started. For example, the primary may crash and quickly restart without missing sending a single Ping.

//! Hint: study the test cases before you start programming. If you fail a test, you may have to look at the test code in test_test.go to figure out the failure scenario is.

// Remember that the Go RPC server framework starts a new thread for each received RPC request. Thus if multiple RPCs arrive at the same time (from multiple clients), there may be multiple threads running concurrently in the server.
// The tests kill a server by setting its dead flag. You must make sure that your server terminates when that flag is set (test it with isdead()), otherwise you may fail to complete the test cases.

package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	ping map[string]time.Time // keep track of the most recent time about a Ping from each server
	view View                 // keep track of the current view
	ack  bool                 // keep track of whether current view has been acknowledged
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	//!!! null ping
	if "" == args.Me {
		return nil
	}

	if 0 == vs.view.Viewnum { // first starts
		vs.view.Viewnum++
		vs.view.Primary = args.Me
		vs.ping[args.Me] = time.Now()
		vs.ack = false
	} else if args.Me == vs.view.Primary && args.Viewnum == vs.view.Viewnum { // primary
		vs.ping[args.Me] = time.Now()
		vs.ack = true
	} else if args.Me == vs.view.Backup && args.Viewnum == vs.view.Viewnum { // Backup
		vs.ping[args.Me] = time.Now()
	} else if "" == vs.view.Backup && true == vs.ack { // update backup whatever viewnum
		vs.view.Viewnum++
		vs.view.Backup = args.Me
		vs.ping[args.Me] = time.Now()
		vs.ack = false
	}
	reply.View = vs.view

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = vs.view
	vs.mu.Unlock()
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()

	var pInterval, bInterval time.Duration
	if "" != vs.view.Primary {
		pInterval = time.Since(vs.ping[vs.view.Primary]) / PingInterval
	}
	if "" != vs.view.Backup {
		bInterval = time.Since(vs.ping[vs.view.Backup]) / PingInterval
	}

	if bInterval >= DeadPings && pInterval >= DeadPings { //primary and backup both dead
		vs.view.Viewnum++
		delete(vs.ping, vs.view.Backup)
		vs.view.Backup = ""
		delete(vs.ping, vs.view.Primary)
		vs.view.Primary = ""
	} else if true == vs.ack { //the primary acknowledges
		if bInterval >= DeadPings { // backup dead
			vs.ack = false
			vs.view.Viewnum++
			delete(vs.ping, vs.view.Backup)
			vs.view.Backup = ""
		}
		if pInterval >= DeadPings { // primary dead
			vs.ack = false
			vs.view.Viewnum++
			delete(vs.ping, vs.view.Primary)
			if "" == vs.view.Backup {
				vs.view.Primary = ""
			} else {
				vs.view.Primary = vs.view.Backup
				vs.view.Backup = ""
			}
		}
	}

	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.ping = make(map[string]time.Time)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
