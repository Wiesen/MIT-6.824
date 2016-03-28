package pbservice

//1. You should start by modifying pbservice/server.go to Ping the viewservice to find the current view.
//   Do this in the *tick()* function.
//   Once a server knows the current view, it knows if it is the primary, the backup, or neither.
//2. Implement *Get, Put, and Append handlers* in pbservice/server.go;
//   store keys and values in a *map[string]string*.
//   If a key does not exist, Append should use an empty string for the previous value.
//   Implement the client.go RPC stubs.
//3. Modify your *handlers* so that the primary forwards updates to the backup.
//4. When a server becomes the backup in a new view,
//   the primary should *send* it the primary's complete key/value database.

//1. Hint: you'll probably need to *create new RPCs to forward* client requests from primary to backup,
//         since the backup should reject a direct client request but should accept a forwarded request.
//2. Hint: you'll probably need to *create new RPCs to handle* the transfer of the complete key/value database from the primary to a new backup.
//         You can send the whole database in one RPC (for example, include a map[string]string in the RPC arguments).
//3. Hint: *the state to filter duplicates* must be replicated along with *the key/value state*.
//4. Hint: the tester arranges for RPC replies to be lost in tests whose description includes "unreliable".
//         This will cause RPCs to be executed by the receiver,
//         but since the sender sees no reply, it cannot tell whether the server executed the RPC.
//5. Hint: you may need to *generate numbers* that have a high probability of being unique.
//6. Hint: the tests kill a server by setting its *dead flag*.
//         You must make sure that your server terminates correctly when that flag is set, otherwise you may fail to complete the test cases.
//7. Hint: even if your viewserver passed all the tests in Part A, it *may still have bugs* that cause failures in Part B.

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk

	// Your declarations here.
	view         viewservice.View  // keep trace of the current view
	kvDatabase   map[string]string // a simple key/value database
	tags         map[int64]bool    // filter duplicates along witth kv state
	unPingCounts int32
}

func (pb *PBServer) put(key string, newVal string) {
	pb.kvDatabase[key] = newVal
}

func (pb *PBServer) append(key string, newVal string) {
	if _, ok := pb.kvDatabase[key]; ok {
		pb.kvDatabase[key] += newVal
	} else {
		pb.kvDatabase[key] = newVal
	}
}

func (pb *PBServer) isPrimary() bool {
	return pb.me == pb.view.Primary
}

func (pb *PBServer) isBackup() bool {
	return pb.me == pb.view.Backup
}

func (pb *PBServer) hasBackup() bool {
	return "" != pb.view.Backup
}

func (pb *PBServer) getBackup() string {
	return pb.view.Backup
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		reply.Value = ""
	} else {
		if pb.forward(&ForwardArgs{Key: args.Key, Operator: "Get"}) {
			if value, ok := pb.kvDatabase[args.Key]; ok {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Value = ""
				reply.Err = OK
			}
		} else {
			reply.Err = ErrUanbleForward
		}
	}
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
	} else {
		key, value, operator, rand := args.Key, args.Value, args.Operator, args.Rand
		if pb.forward(&ForwardArgs{Key: key, Value: value, Operator: operator, Rand: rand}) {
			if _, ok := pb.tags[rand]; !ok {
				pb.tags[rand] = true
				switch operator {
				case "Put":
					pb.put(key, value)
				case "Append":
					pb.append(key, value)
				}

				//fmt.Printf("[PutAppend] host [%v]: [%v] -> [%v]\n", pb.me, key, value)
			}
			reply.Err = OK
		} else {
			reply.Err = ErrUanbleForward
		}
	}
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	currentView, err := pb.vs.Ping(pb.view.Viewnum)
	if err == nil && pb.view.Viewnum != currentView.Viewnum {
		if pb.isPrimary() && pb.getBackup() == "" && "" != currentView.Backup {
			pb.transfer(currentView.Backup)
		}
		pb.view = currentView
		pb.unPingCounts = 0
	} else if err != nil && pb.unPingCounts < viewservice.DeadPings {
		if pb.unPingCounts += 1; pb.unPingCounts >= viewservice.DeadPings {
			pb.view = viewservice.View{}
		}
	}
}

//* New RPC
//  transfer the complete key/value database from the primary to a new backup
func (pb *PBServer) transfer(srv string) {
	args := &TransferArgs{pb.kvDatabase, pb.tags}
	var reply TransferReply
	call(srv, "PBServer.Update", args, &reply)
}

func (pb *PBServer) Update(args *TransferArgs, reply *TransferReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.kvDatabase = args.KVDatabase
	pb.tags = args.Tags
	reply.Err = OK
	return nil
}

//* New RPC
//  forward client requests from primary to backup
func (pb *PBServer) forward(args *ForwardArgs) bool {
	var reply ForwardReply
	if backup := pb.getBackup(); "" != backup {
		return call(backup, "PBServer.Accept", args, &reply)
	}
	return true
}

func (pb *PBServer) Accept(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	key, value, operator, rand := args.Key, args.Value, args.Operator, args.Rand
	if _, ok := pb.tags[rand]; !ok {
		pb.tags[rand] = true
		switch operator {
		case "Put":
			pb.put(key, value)
		case "Append":
			pb.append(key, value)
		}
	}

	reply.Err = OK
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.kvDatabase = make(map[string]string)
	pb.tags = make(map[int64]bool)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
