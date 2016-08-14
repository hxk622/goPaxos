package main

import "goPaxos"

func main() {
	//create 2 proposer
	proposers := []*goPaxos.Proposer{}
	for i:=0; i<2; i++ {
		proposers = append(proposers, goPaxos.NewProposer(i))
	}
	//create 3 accetpor
	acceptors := []*goPaxos.Acceptor{}
	for i:=0; i<3; i++ {
		acceptors = append(acceptors, goPaxos.NewAcceptor(i))
	}

	//begin proposal
	goPaxos.GlobalProposerWg.Add(1)
	proposers[0].SendPrepareRequest()
	goPaxos.GlobalProposerWg.Wait()

}