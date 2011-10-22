package tribproto

// Status codes
const (
	OK = iota
	ENOSUCHUSER
	ENOSUCHTARGETUSER
	EEXISTS // for Create user only
)
type Tribble struct {
	Userid string  // user who posted the Tribble
	Posted int64   // Posting time, from Time.Nanoseconds()
	Contents string
}

type CreateUserArgs struct {
	Userid string
}

type CreateUserReply struct {
	Status int
}

type PostTribbleArgs struct {
	Userid string
	Contents string
}

type PostTribbleReply struct {
	Status int
}

type SubscriptionArgs struct {  // For both and add remove
	Userid string
	Targetuser string
}

type SubscriptionReply struct {
	Status int
}

type GetSubscriptionsArgs struct {
	Userid string
}

type GetSubscriptionsReply struct {
	Status int
	Userids []string
}

type GetTribblesArgs struct { // Used for both GetTribbles and GetTribblesBySubscription
	Userid string
}

type GetTribblesReply struct {
	Status int
	Tribbles []Tribble
}


