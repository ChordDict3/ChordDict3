package main

import (
	"fmt"
	"net"
	"encoding/json"
	//"io/ioutil"
	"os"
	//"github.com/HouzuoGuo/tiedot/db"
	"strconv"
	"math"
)



type ChordNode struct{
	Identifier string
	Successor string
	Predecessor string
	HashID string
	Bootstrap string     // Another node it knows about to join the ring
	Finger map[int]string
	M int
}

type Request struct{
	Method string `json:"method"` 
	Params interface{} `json:"params"`
}

type Response struct{
	Result interface{} `json:"result"`
	Error interface{} `json:"error"`
}


type TestConfiguration struct{
	ServerID string 
	Protocol string 
	IpAddress string
	Port string

}

func createConnection(identifer string) (encoder *json.Encoder, decoder *json.Decoder){

	//TODO - either have method for identifer -> ip:port, or track this from beginning
	
	conn, err := net.Dial("tcp", "localhost:1000"+identifer)
	if err != nil {
		fmt.Println("Connection error", err)
	}
	e := json.NewEncoder(conn)
	d := json.NewDecoder(conn)
	
	return e, d
}


func join(node *ChordNode)(){
	encoder, decoder := createConnection(node.Bootstrap)
	m := Request{"find_successor", node.Identifier}
	encoder.Encode(m)

	res := new(Response)
	decoder.Decode(&res)
	fmt.Println("Recieved: %+v", res)
	
}

func find_successor(node *ChordNode, req *Request, encoder *json.Encoder){
	//return node.Finger[1]?

	//find closest, ask them where its at
	target := req.Params.(string)
	target_val, _ := strconv.Atoi(target)
	
	previous_finger := -1
	current_finger := -1
	successor := -1
	for i :=node.M; i >= 0 ; i-- {
		current_finger = strconv.Atoi(node.Finger[math.Pow(2,i)])
		if target_val == current_finger {
			//This is where the target should be at
			successor = current_finger
		}else if previous_finger != -1 {
			if target_val > current_finger && target_val < previous_finger {
				successor = previous_finger
				break
			}
		}
		previous_finger = current_finger
	}

	//Guessing here
	if successor == -1 {
		//Assign highest m value to look at
		successor = node.Finger[math.Pow(2,node.M)]
	}
	

	
}

func readTestConfig()(config *TestConfiguration){

	config = new(TestConfiguration)
	config.IpAddress = "127.0.0.1"
	config.Protocol = "tcp"

	return config

}

func handleConnection(node *ChordNode, conn net.Conn){
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	req := new(Request)
	decoder.Decode(&req)

	switch req.Method {
	case "find_successor" :
		find_successor(node, req, encoder)
	}
		

}

func main() {
	

	// Parse argument configuration block
	node := new(ChordNode)
	config := readTestConfig()

	// For early development, the chord node listens on 1000x, where x is its identifier
	// The identifier is passed as the first command line argument
	// The bootstrap (another node to use for join()) is the second optional argument

	node.Identifier = os.Args[1]
	if len(os.Args > 2){
		node.Bootstrap = os.Args[2]
	} else {
		node.Bootstrap = "-1"
	}

	node.M = 3 // m-bit Key Space
	node.HashID = node.Identifier
	//node.Successor = node.Bootstrap
	config.Port = "1000" + node.Identifier


	//Join the ring

	if node.Identifer != node.Bootstrap {
		join(node)
	} else {
		for i := 1; i < m; i++ {
			node.Finger[math.Pow(2,i)] = node.Identifier 
		}
	}

			
	

	networkaddress := config.IpAddress + ":" + config.Port
	ln, err := net.Listen(config.Protocol, networkaddress)
	if err != nil {
		// handle error
	}
	for {
		conn, err := ln.Accept() // this blocks until connection or error
		if err != nil {
			// handle error
			continue
		}
		go handleConnection(node, conn) 
	}
	
}
