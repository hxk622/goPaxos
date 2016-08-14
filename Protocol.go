package goPaxos

import (
	"common"
	"sync"
	"time"
)

/*
	gen global unique num
	global-num generate principle: n*i+j (n代表Proposer数量，i代表是Proposer决议次数，j代表Proposer编号，从0开始)
*/
var (
	ServerNum        = 5
	GlobalPaxosRoute = NewPaxosRoute()
	GlobalProposerWg = &sync.WaitGroup{}
	EmptyValue       = -1
)

func GenGlobalNum(i, j int) int {
	return ServerNum*i + j
}

/*
	消息类型
*/
type MsgType int

const (
	PrepareRequest MsgType = iota
	PrepareResponse
	AcceptRequest
	AcceptResponse
)

/*
	pasox角色
*/
type Role int

const (
	ProposerRole Role = iota
	AcceptorRole
	LearnerRole
	ClientRole
)

/*
	register the Acceptor's count(alive Table)
*/
type PaxosRoute struct {
	sync.RWMutex
	ProposerRoute map[int]chan []byte
	AcceptorRoute map[int]chan []byte
}

func NewPaxosRoute() *PaxosRoute {
	return &PaxosRoute{
		ProposerRoute: make(map[int]chan []byte),
		AcceptorRoute: make(map[int]chan []byte),
	}
}
func (this *PaxosRoute) Register(role Role, key int, value interface{}) bool {
	this.Lock()
	defer this.Unlock()
	if role == AcceptorRole {
		this.AcceptorRoute[key] = value.(chan []byte)
	} else if role == ProposerRole {
		this.ProposerRoute[key] = value.(chan []byte)
	}
	return true
}
func (this *PaxosRoute) GetAcceptorRoute() map[int]chan []byte {
	this.RLock()
	defer this.RUnlock()
	return this.AcceptorRoute
}

//protocol
/*
request protocol
{
	type : prepare | accept
	N : int
	V : int
}
response protocol
{
	ok bool
	acceptN int
	acceptV int
}
*/

type RequestProtocol struct {
	ProposerId int
	AcceptorId int
	MsgType    MsgType //prepare or accept
	N          int
	V          int
	SendTime   time.Time
}

func NewReqestProtocol(proposerId, acceptorId, n, v int, msgType MsgType) *RequestProtocol {
	return &RequestProtocol{
		ProposerId: proposerId,
		AcceptorId: acceptorId,
		N:          n,
		V:          v,
		MsgType:    msgType,
		SendTime:   time.Now(),
	}
}
func (this RequestProtocol) Encode() []byte {
	data, _ := common.ToMsgPack(this)
	return data
}

func (this *RequestProtocol) Decode(data []byte) error {
	return common.FromMsgPack(data, this)
}

type ResponseProtocol struct {
	Ok         bool
	ProposerId int
	AcceptorId int
	MsgType    MsgType
	AcceptN    int
	AcceptV    int
	SendTime   time.Time
}

func NewResponseProtocol(ok bool, proposerId, acceptorId, acceptN, acceptV int, msgType MsgType) *ResponseProtocol {
	return &ResponseProtocol{
		Ok:         ok,
		ProposerId: proposerId,
		AcceptorId: acceptorId,
		AcceptN:    acceptN,
		AcceptV:    acceptV,
		MsgType:    msgType,
		SendTime:   time.Now(),
	}
}
func (this ResponseProtocol) Encode() []byte {
	data, _ := common.ToMsgPack(this)
	return data
}

func (this *ResponseProtocol) Decode(data []byte) *ResponseProtocol {
	common.FromMsgPack(data, this)
	return this
}
