package goPaxos

import (
	"fmt"
	"sync"
)

type Proposer struct {
	ProposerId int
	RoundNum   int
	channel    chan []byte
	wg         *sync.WaitGroup
}

func NewProposer(proposerId int) *Proposer {
	obj := &Proposer{
		ProposerId: proposerId,
		RoundNum:   0,
		channel:    make(chan []byte, ServerNum),
		wg:         &sync.WaitGroup{},
	}
	GlobalPaxosRoute.Register(ProposerRole, obj.ProposerId, obj.channel)
	return obj
}

//workflow: 2-phrase commit
/*
 phrase-1
*/
func (this Proposer) SendPrepareRequest() {
	//gen N
	this.RoundNum += 1
	fmt.Println("begin round:", this.RoundNum)
	N := GenGlobalNum(this.RoundNum, this.ProposerId)
	V := this.ProposerId

	//send to all acceptor
	allResponse := []*ResponseProtocol{}
	acceptorRoutes := GlobalPaxosRoute.AcceptorRoute
	for acceptorId, channel := range acceptorRoutes {
		this.wg.Add(1)
		go func(acceptorId int, channel chan []byte) {
			fmt.Println("send prepare request to acceptor ", acceptorId, "acceptN ",N,"acceptV ",V)
			prepareRequestMsg := NewReqestProtocol(this.ProposerId, acceptorId, N, V, PrepareRequest)
			channel <- prepareRequestMsg.Encode()
			prepareResponseBuf := <-this.channel
			responseProtocol := &ResponseProtocol{}
			allResponse = append(allResponse, responseProtocol.Decode(prepareResponseBuf))
			this.wg.Done()
		}(acceptorId, channel)
	}

	//wait response
	this.wg.Wait()

	acceptN, acceptV := this.ParsePrepareResponse(allResponse)
	if acceptN == EmptyValue || acceptV == EmptyValue {
		fmt.Println("prepare response is error, need retry proposal")
		//go this.SendPrepareRequest()
	} else {
		//send phrase-2
		this.SendAcceptRequest(acceptN, acceptV)
	}
}

/*
	parse phrase-1's response
	超过半数的pok，则从acceptN中选择最大的那个acceptV，如果acceptN全是EmptyValue，则acceptV为proposerId
	未超过半数，则重新发起phrase-1
*/
func (this Proposer) ParsePrepareResponse(allResponse []*ResponseProtocol) (int, int) {
	//lookup majority acceptN
	//map : key acceptN acceptV
	//map : key acceptN count
	kvMap := make(map[int]int)
	maxn, maxc := 0, 0
	var allNull = true
	for _, response := range allResponse {
		acceptN, acceptV := response.AcceptN, response.AcceptV
		fmt.Println("prepare response, acceptN, acceptV:", acceptN, acceptV)
		if response.Ok {
			maxc++
			if acceptN == EmptyValue && acceptV == EmptyValue {
				continue
			} else {
				allNull = false
				kvMap[acceptN] = acceptV
				if maxn < response.AcceptN {
					maxn = response.AcceptN
				}
			}
		}
	}
	fmt.Println(maxc)
	if maxc > ServerNum/2 {
		if allNull == true {
			return this.RoundNum, this.ProposerId
		} else {
			return maxn, kvMap[maxn]
		}
	} else {
		return EmptyValue, EmptyValue
	}
}

/*
	phrase-2
*/
func (this Proposer) SendAcceptRequest(acceptN, acceptV int) {
	//send
	allResponse := []*ResponseProtocol{}
	acceptorRoutes := GlobalPaxosRoute.AcceptorRoute
	for acceptorId, channel := range acceptorRoutes {
		this.wg.Add(1)
		go func() {
			fmt.Println("send prepare request to acceptor-", acceptorId)
			prepareRequestMsg := NewReqestProtocol(this.ProposerId, acceptorId, acceptN, acceptV, AcceptRequest)
			channel <- prepareRequestMsg.Encode()
			prepareResponseBuf := <-this.channel
			responseProtocol := &ResponseProtocol{}
			allResponse = append(allResponse, responseProtocol.Decode(prepareResponseBuf))
			this.wg.Done()
		}()
	}
	this.wg.Wait()
	//parse
	ok := this.ParseAcceptResponse(allResponse)
	if ok {
		this.NoticeLearner(acceptN, acceptV)
	} else {
		fmt.Println("accept response is error, need retry proposal")
		go this.SendPrepareRequest()
	}
}

/*
   parse phrase-2's response
   超过半数ok，则这次accept成功，将acceptN和acceptV通知learner
   否则，重新发起phrase-1
*/
func (this Proposer) ParseAcceptResponse(allResponse []*ResponseProtocol) bool {
	maxc := 0
	for _, response := range allResponse {
		if response.Ok {
			maxc++
		}
	}
	if maxc > ServerNum/2 {
		return true
	} else {
		return false
	}
}

func (this Proposer) NoticeLearner(acceptN, acceptV int) {
	fmt.Println("acceptN:", acceptN, " acceptV:", acceptV)
	GlobalProposerWg.Done()
}
