package storageproto

// Status codes
const (
	OK = iota
	EKEYNOTFOUND
	EITEMNOTFOUND // lists
	EPUTFAILED
	EITEMEXISTS
)

type GetArgs struct {
	Key string
}

type GetReply struct {
	Status int
	Value string
}

type GetListReply struct {
	Status int
	Value []string
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

func (c Client) Less(y interface{}) bool {
        return c.NodeID < y.(Client).NodeID
}

type RegisterArgs struct {
	ClientInfo Client
}
type RegisterReply struct {
	Ready bool
	Clients []Client 
}
