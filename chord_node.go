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



/////
// RPC Functions
/////

func get_succcessor(node *ChordNode, req *Request, encoder *json.Encoder){
	m := Response{node.Successor, nil}
	fmt.Println("Response to get_successor for",m)
	encoder.Encode(m)
	return

}

func get_predecessor(node *ChordNode, req *Request, encoder *json.Encoder){
	m := Response{node.Predecessor, nil}
	fmt.Println("Response to get_predecessor for",m)
	encoder.Encode(m)
	return
}

func set_predecessor(node *ChordNode, req *Request, encoder *json.Encoder){
	id, _ := strconv.Atoi(req.Params.(string))
	node.Predecessor = id
	
	m := Response{nil, nil}
	fmt.Println("Response to get_predecessor for",m)
	encoder.Encode(m)
	return
}

func set_successor(node *ChordNode, req *Request, encoder *json.Encoder){
	id, _ := strconv.Atoi(req.Params.(string))
	node.Successor = id
	
	m := Response{nil, nil}
	fmt.Println("Response to get_predecessor for",m)
	encoder.Encode(m)
	return
}



func find_predecessor(node *ChordNode, req *Request, encoder *json.Encoder){

	fmt.Println("here")
	
	id, _ := strconv.Atoi(req.Params.(string))

	if (id == node.Identifier) || (id > node.Identifier && id < node.Successor) || node.Identifier == node.Successor {
		m := Response{node.Identifier, nil}
		fmt.Println("Response to ",id," ",m)
		encoder.Encode(m)
	} else {
			
		for i :=0; i > node.M ; i++ {
			current_finger := node.Finger[node.Identifier + powerof(2,i) % powerof(2,node.M)]
			lower_interval := node.Identifier + powerof(2,i) % powerof(2,node.M)
			upper_interval := node.Identifier + powerof(2,i+1) % powerof(2,node.M)
			
			if i == id {
				m := Response{current_finger, nil}
				fmt.Println("Response to ",id," ", m)
				encoder.Encode(m)
				return
			}else{
				//Is id in interval for this finger entry
				if id > lower_interval && id < upper_interval {
					m := Response{current_finger, nil}
					fmt.Println("Response to ",id, " ",m)
					encoder.Encode(m)
					return
				}
				
			}
			
			
		}
		

	}
	

}

func find_successor(node *ChordNode, req *Request, encoder *json.Encoder){
	id, _ := strconv.Atoi(req.Params.(string))
	
	encoder_predecessor, decoder_predecessor := createConnection(node.Identifier)
	m_predecessor := Request{"find_predecessor", id}
	encoder_predecessor.Encode(m)

	res_predecessor := new(Response)
	decoder_predecessor.Decode(&res_predecessor)
	fmt.Println("Recieved: ", res_predecessor)

	result := strconv.Atoi(res_predecessor.Result.(string))

	//One more call here to get the successor??

	found_successor := ""
	
	if result == node.Identifer {
		//This node can return its own successor
		found_successor = node.Successor
	} else {
		//We know the predecessor, now we ask that predecessor for its successor
		encoder_successor, decoder_successor := createConnection(result)
		m_successor := Request{"get_succcessor", nil}
		new_encoder.Encode(m)
		
		res_successor := new(Response)
		decoder_successor.Decode(&res)
		fmt.Println("Recieved: ", res)
		
		found_successor := strconv.Atoi(res_successor.Result.(string))

	
	}

	m := Response{found_successor, nil}
	fmt.Println("Response to find_successor for",id, " ",m)
	encoder.Encode(m)
	return

}

/////
// Helper Functions
/////

func powerof(x int, y int)(val int){
	val = int(math.Pow(float64(x),float64(y)))
	return val
}

/////
// Node setup Functions
/////



func readTestConfig()(config *TestConfiguration){

	config = new(TestConfiguration)
	config.IpAddress = "127.0.0.1"
	config.Protocol = "tcp"

	return config

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

	init_finger_table(node)
	
}


func init_finger_table(node *ChordNode)(){
	//Asking bootstrap for successor of beginning of the the first finger's starting interval
	start = node.Identifier + powerof(2,0) % powerof(2,node.M)
	
	encoder, decoder := createConnection(node.Bootstrap)
	m := Request{"find_predecessor", node.start}
	encoder.Encode(m)


	res := new(Response)
	decoder.Decode(&res)
	fmt.Println("Recieved: ", res)
	node.successor := strconv.Atoi(res_successor.Result.(string))
	
	//first finger is successor
	node.Finger[node.Identifier + powerof(2,0) % powerof(2,node.M)] = node.Successor

	//get this successor's predecessor, which becomes the current node's predecessor
	encoder, decoder := createConnection(node.Successor)
	m := Request{"get_predecessor", nil}
	encoder.Encode(m)

	res := new(Response)
	decoder.Decode(&res)
	fmt.Println("Recieved: ", res)
	node.predecessor := strconv.Atoi(res_successor.Result.(string))

	//set the successor's predecessor to the current node
	encoder, decoder := createConnection(node.Successor)
	m := Request{"set_predecessor", node.Identifier}
	encoder.Encode(m)

	res := new(Response)
	decoder.Decode(&res)
	fmt.Println("Recieved: ", res)

	//loop through to update fingars
	

}


//////
// Handle incoming requests for RPCs
//////


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
	case "get_successor" :
		get_successor(node, req, encoder)
	case "get_predecessor" :
		get_predecessor(node, req, encoder)
	case "set_successor" :
		set_successor(node, req, encoder)
	case "set_predecessor" :
		set_predecessor(node, req, encoder)
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
	//TODO in the future: node.HashID = generateNodeHash(node.Identifier)
	
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
