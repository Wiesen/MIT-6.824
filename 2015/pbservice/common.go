package pbservice

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongServer   = "ErrWrongServer"
	ErrUanbleForward = "ErrUnableForward"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operator string
	Rand     int64
}

type PutAppendReply struct {
	Err Err
}

// Get
type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.

// Forward
type ForwardArgs struct {
	Key      string
	Value    string
	Operator string
	Rand     int64
}

type ForwardReply struct {
	Err Err
}

// Transfer
type TransferArgs struct {
	KVDatabase map[string]string
	Tags       map[int64]bool
}

type TransferReply struct {
	Err Err
}
