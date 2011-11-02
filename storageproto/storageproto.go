package storageproto

// Status codes
const (
	OK = iota
	EKEYNOTFOUND
	EITEMNOTFOUND // lists
	EPUTFAILED
	EITEMEXISTS // lists, duplicate put
)

type LeaseStruct struct {
	Granted bool
	ValidSeconds int
}

type GetArgs struct {
	Key string
	WantLease bool
	LeaseClient Client // StorageServer that wants the lease
}

type GetReply struct {
	Status int
	Value string
	Lease LeaseStruct
}

type GetListReply struct {
	Status int
	Value []string
	Lease LeaseStruct
}

type PutArgs struct {
	Key string
	Value string
}

type PutReply struct {
	Status int
}

type Client struct {
	HostPort string
	NodeID uint32
}

type RegisterArgs struct {
	ClientInfo Client
}
type RegisterReply struct {
	Ready bool
	Clients []Client 
}

type RevokeLeaseArgs struct {
	Key string
}

type RevokeLeaseReply struct {
	Status int
}
