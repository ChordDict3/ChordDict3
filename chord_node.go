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
	Identifier int
	Successor int
	Predecessor int
	HashID int
	Bootstrap int     // Another node it knows about to join the ring
	Finger map[int]int
	M int
	Keys []int
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

func createConnection(identifer int) (encoder *json.Encoder, decoder *json.Decoder){
	
	conn, err := net.Dial("tcp", "localhost:1000"+ strconv.Itoa(identifer))
	if err != nil {
		fmt.Println("Connection error", err)
	}
	e := json.NewEncoder(conn)
	d := json.NewDecoder(conn)
	
	return e, d
}


func join(node *ChordNode)(){
	encoder, decoder := createConnection(node.Bootstrap)
	id := strconv.Itoa(node.Identifier)
	m := Request{"find_predecessor", id}
	encoder.Encode(m)

	res := new(Response)
	decoder.Decode(&res)
	fmt.Println("Recieved: ", res)
	
}

func find_predecessor(node *ChordNode, req *Request, encoder *json.Encoder){

	fmt.Println("here")
	
	target, _ := strconv.Atoi(req.Params.(string))

	if (target == node.Identifier) || (target > node.Identifier && target < node.Successor) || node.Identifier == node.Successor {
		m := Response{node.Identifier, nil}
		fmt.Println("Response to ",target," ",m)
		encoder.Encode(m)
	} else {
			
		for i :=0; i > node.M ; i++ {
			current_finger := node.Finger[node.Identifier + powerof(2,i) % powerof(2,node.M)]
			lower_interval := node.Identifier + powerof(2,i) % powerof(2,node.M)
			upper_interval := node.Identifier + powerof(2,i+1) % powerof(2,node.M)
			
			if i == target {
				m := Response{current_finger, nil}
				fmt.Println("Response to ",target," ", m)
				encoder.Encode(m)
				return
			}else{
				//Is target in interval for this finger entry
				if target > lower_interval && target < upper_interval {
					m := Response{current_finger, nil}
					fmt.Println("Response to ",target, " ",m)
					encoder.Encode(m)
					return
				}
				
			}
			
			
		}
		

	}
	

}

func find_successor(node *ChordNode, req *Request, encoder *json.Encoder){
	fmt.Println("not implemented")


	
}

func powerof(x int, y int)(val int){
	val = int(math.Pow(float64(x),float64(y)))
	return val
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

	fmt.Println(req)
	
	switch req.Method {
	case "find_successor" :
		find_successor(node, req, encoder)
	case "find_predecessor" :
		find_predecessor(node, req, encoder)
	}

		

}

func main() {
	

	// Parse argument configuration block
	node := new(ChordNode)
	config := readTestConfig()

	// For early development, the chord node listens on port 1000x, where x is its identifier
	// The identifier is passed as the first command line argument
	// The bootstrap (another node to use for join()) is the second optional argument

	node.Identifier, _ = strconv.Atoi(os.Args[1])
	if len(os.Args) > 2 {
		node.Bootstrap, _ = strconv.Atoi(os.Args[2])
	} else {
		node.Bootstrap = node.Identifier
	}

	node.M = 3 // m-bit Key Space
	node.HashID = node.Identifier
	//node.Successor = node.Bootstrap
	node.Finger = make(map[int]int)
	config.Port = "1000" + strconv.Itoa(node.Identifier)

	//For testing give node a key
	if node.Identifier == 1 {
		node.Keys = make([]int, 1)
		node.Keys[0] = 1
	}
	
	
	//Join the ring
	//Need to intelize with modulo 
	if node.Identifier != node.Bootstrap {
		join(node)
	} else {
		//The node is the only one; init finger table, successor, predecessor
		for i := 0; i < node.M; i++ {
			node.Finger[node.Identifier + powerof(2,i) % powerof(2,node.M)] = node.Identifier 
		}
		//finger[0]
		node.Successor = node.Finger[node.Identifier + powerof(2,0) % powerof(2,node.M)]
		//
		node.Predecessor = node.Identifier
	}


	fmt.Println(node.Finger)
	

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
