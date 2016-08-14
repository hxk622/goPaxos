package goPaxos

import "fmt"

type Acceptor struct {
	AcceptorId int
	MaxN       int
	AcceptN    int
	AcceptV    int
	Channel    chan []byte
}

func NewAcceptor(acceptorId int) *Acceptor {
	obj := &Acceptor{
		AcceptorId: acceptorId,
		MaxN:       EmptyValue,
		AcceptN:    EmptyValue,
		AcceptV:    EmptyValue,
		Channel:    make(chan []byte, ServerNum),
	}
	GlobalPaxosRoute.Register(AcceptorRole, obj.AcceptorId, obj.Channel)
	go func() {
		for {
			obj.WaitProposal()
		}
	}()
	return obj
}

func (this *Acceptor) WaitProposal() {
	//wait proposal
	data := <-this.Channel
	proposalRequest := &RequestProtocol{}
	proposalRequest.Decode(data)
	channel := GlobalPaxosRoute.ProposerRoute[proposalRequest.ProposerId]
	N := proposalRequest.N
	V := proposalRequest.V
	//check msgType
	if proposalRequest.MsgType == PrepareRequest {
		if this.MaxN == EmptyValue {
			fmt.Println("acceptorId:",proposalRequest.AcceptorId," aaa:", N," ",this.MaxN)
			this.MaxN = N
			this.AcceptN = N
			this.AcceptV = V
			resp := NewResponseProtocol(true, proposalRequest.ProposerId, proposalRequest.AcceptorId, this.AcceptN, this.AcceptV, PrepareResponse)
			channel <- resp.Encode()
			return
		} else if N <= this.MaxN {
			fmt.Println("acceptorId:",proposalRequest.AcceptorId," bbb:", N," ",this.MaxN)
			resp := NewResponseProtocol(false, proposalRequest.ProposerId, proposalRequest.AcceptorId, EmptyValue, EmptyValue, PrepareResponse)
			channel <- resp.Encode()
			return
		} else {
			fmt.Println("acceptorId:",proposalRequest.AcceptorId," ccc:", N," ",this.MaxN)
			this.MaxN = N
			resp := NewResponseProtocol(true, proposalRequest.ProposerId, proposalRequest.AcceptorId, this.AcceptN, this.AcceptV, PrepareResponse)
			channel <- resp.Encode()
			return
		}
	} else if proposalRequest.MsgType == AcceptRequest {
		if N < this.MaxN {
			resp := NewResponseProtocol(false, proposalRequest.ProposerId, this.AcceptorId, EmptyValue, EmptyValue, AcceptResponse)
			channel <- resp.Encode()
			return
		} else {
			this.MaxN = N
			this.AcceptN = N
			this.AcceptV = V
			resp := NewResponseProtocol(true, proposalRequest.ProposerId, this.AcceptorId, this.AcceptN, this.AcceptV, AcceptResponse)
			channel <- resp.Encode()
			return
		}
	}
}
