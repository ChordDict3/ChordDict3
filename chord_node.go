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
	//"time"
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

func get_successor(node *ChordNode, req *Request, encoder *json.Encoder){
	m := Response{node.Successor, nil}
	encoder.Encode(m)
	return

}

func get_predecessor(node *ChordNode, req *Request, encoder *json.Encoder){
	status(node, "sending predecessor" + strconv.Itoa(node.Predecessor))
	m := Response{node.Predecessor, nil}
	encoder.Encode(m)
	return
}

func set_predecessor(node *ChordNode, req *Request, encoder *json.Encoder){
	id := int(req.Params.(float64))
	node.Predecessor = id
	status(node, "set_predecessor")
	m := Response{nil, nil}
	encoder.Encode(m)
	return
}

func set_successor(node *ChordNode, req *Request, encoder *json.Encoder){
	id := int(req.Params.(float64))
	node.Successor = id
	
	status(node, "Successor updated")
	
	
	m := Response{nil, nil}
	encoder.Encode(m)
	return
}

func update_finger_table(node *ChordNode, req *Request, encoder *json.Encoder){
	
	arr := req.Params.([]interface{})
	
	s := int(arr[0].(float64))
	i := int(arr[1].(float64))

	status(node, "update_finger_table")
	//set Successor here if Finger[0] gets updated
	//The second check is to cover if the interval wraps around
	
	//TODO - make this cleaner
	if (s > node.Identifier && s < node.Finger[node.Identifier + powerof(2,i-1) % powerof(2,node.M)]) || ( (node.Identifier < s) && ((s > node.Identifier) && (s < node.Finger[node.Identifier + powerof(2,i-1) % powerof(2,node.M)] + powerof(2,node.M))))    {
		
		status(node, "update_finger_table Finger being updated")
		node.Finger[node.Identifier + powerof(2,i-1) % powerof(2,node.M)] = s
				
		//Updating Successor
		if i == 1 {
			node.Successor = node.Finger[node.Identifier + powerof(2,i-1) % powerof(2,node.M)]
		}

		arr = []interface{}{s, i}
		encoder_predecessor, decoder_predecessor := createConnection(node.Predecessor)
		m_predecessor := Request{"update_finger_table", arr}
		encoder_predecessor.Encode(m_predecessor)
		
		res := new(Response)
		decoder_predecessor.Decode(&res)


	} 

	status(node, "update_finger_table finished")
	
	m := Response{nil, nil}
	encoder.Encode(m)
	return

	
	
}



/*
closest_preceding_finger

if finger[i].node in (n, id):
return finger[i].node

*/


func closest_preceding_finger(node *ChordNode, id int)(current_finger int){

	 
 	for i := node.M - 1; i >= 0 ; i-- {
		current_finger = node.Finger[node.Identifier + powerof(2,i) % powerof(2,node.M)]

		fmt.Println("ident ", node.Identifier, "CF ", current_finger, "id ", id)

		//Have to handle the wrap around case too

		//lower_bound = 1
		//upper_bound = 0
		//current_finger = 0
		

		
		lower_bound := node.Identifier
		upper_bound := id
		

		if current_finger > lower_bound && current_finger <= upper_bound {
			return current_finger
		}
		
		//wrap around case, current_finger is not wrapped around
		if upper_bound < lower_bound {
		
			if current_finger > lower_bound && current_finger <= upper_bound + powerof(2, node.M) {
				return current_finger
			} 
		}

		//wrap around case, current_finger is wrapped around
		if (upper_bound < lower_bound) && (current_finger <= upper_bound) {
			if (current_finger + powerof(2, node.M) > lower_bound) && (current_finger <= upper_bound) {
				return current_finger
			}
		}

		//if (current_finger > node.Identifier && current_finger <= id) || ((id < node.Identifier) && ((current_finger + powerof(2, node.M) > node.Identifier) && (current_finger + powerof(2, node.M) <= id + powerof(2, node.M)))) {
		//	return current_finger
		//}

	}
	//Somethings wrong
	fmt.Println("closest_preceding_finger failed")
	os.Exit(0)
	return -1
}

func find_predecessor(node *ChordNode, req *Request, encoder *json.Encoder){

	id := int(req.Params.(float64))

	//See if the id is in section of the ring that this node is responsible for
	//Could be improved??
	if id == node.Identifier || id > node.Identifier && id < node.Successor || ((node.Successor < node.Identifier) && ((id > node.Identifier) && (id < node.Successor + powerof(2,node.M)))) || node.Identifier == node.Successor {
		status(node, "find_predecessor "+"id " + strconv.Itoa(id) + " result " + strconv.Itoa(node.Identifier))
		msg := Response{node.Identifier, nil}
		encoder.Encode(msg)
			
	} else {
		//now check the finger table to get a new node to check
		status(node, "calling closest_preceding_finger looking for " + strconv.Itoa(id))
		p := closest_preceding_finger(node, id)

		//now recursively call the closer node
			
		encoder_predecessor, decoder_predecessor := createConnection(p)
		m_predecessor := Request{"find_predecessor", id}
		encoder_predecessor.Encode(m_predecessor)
		
		res_predecessor := new(Response)
		decoder_predecessor.Decode(&res_predecessor)
		
		status(node, "find_predecessor "+"id " + strconv.Itoa(id) + " result " + strconv.Itoa(int(res_predecessor.Result.(float64))))
		msg := Response{res_predecessor.Result.(float64), nil}
		encoder.Encode(msg)
	
	}
}
	



func find_successor(node *ChordNode, req *Request, encoder *json.Encoder){
	id := int(req.Params.(float64))
	
	encoder_predecessor, decoder_predecessor := createConnection(node.Identifier)
	m_predecessor := Request{"find_predecessor", id}
	encoder_predecessor.Encode(m_predecessor)

	res_predecessor := new(Response)
	decoder_predecessor.Decode(&res_predecessor)


	result := int(res_predecessor.Result.(float64))

	found_successor := 0
	
	if result == node.Identifier {
		//This node can return its own successor
		found_successor = node.Successor
	} else {
		//We know the predecessor, now we ask that predecessor for its successor
		encoder_successor, decoder_successor := createConnection(result)
		m_successor := Request{"get_succcessor", nil}
		encoder_successor.Encode(m_successor)
		
		res_successor := new(Response)
		decoder_successor.Decode(&res_successor)

		
		found_successor = int(res_successor.Result.(float64))

	
	}

	m := Response{found_successor, nil}
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

func status(node *ChordNode, msg string){
	fmt.Println("Ident: ", node.Identifier, " P: ", node.Predecessor, " S: ", node.Successor, " F: ", node.Finger, " ",  msg)

} 

//Because golang's % of negative numbers is broken
//https://github.com/golang/go/issues/448
func mod(m int, n int)(val int){
	val = ((m % n) + n) % n
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
	update_others(node)
	status(node, "Finish Join")
	
}


func init_finger_table(node *ChordNode)(){
	//Asking bootstrap for successor of beginning of the the first finger's starting interval
	start := (node.Identifier + powerof(2,0) % powerof(2,node.M))

	encoder, decoder := createConnection(node.Bootstrap)
	m := Request{"find_successor", start}
	encoder.Encode(m)

	res := new(Response)
	decoder.Decode(&res)
	
	node.Successor = int(res.Result.(float64))


	//first finger is successor
	node.Finger[node.Identifier + powerof(2,0) % powerof(2,node.M)] = node.Successor

	//get this successor's predecessor, which becomes the current node's predecessor
	encoder, decoder = createConnection(node.Successor)
	m = Request{"get_predecessor", nil}
	encoder.Encode(m)

	res = new(Response)
	decoder.Decode(&res)
	node.Predecessor = int(res.Result.(float64))
	
	status(node, "Do we have the right predecessor, successor?")


	//set the successor's predecessor to the current node
	encoder, decoder = createConnection(node.Successor)
	m = Request{"set_predecessor", node.Identifier}
	encoder.Encode(m)

	res = new(Response)
	decoder.Decode(&res)

	//loop through finger list to update fingars
	status(node, "looping through finger list to update")
	for i := 1; i < node.M; i++ {

		start = node.Identifier + powerof(2,i) % powerof(2,node.M)
		previous_finger :=  node.Finger[node.Identifier + powerof(2,i-1) % powerof(2,node.M)]

		//if the start of this finger is less that the node for the previous finger (see the i-1), it is also the node for this finger
		//handle wrap-around case

		if (start < node.Finger[previous_finger]) || ((node.Finger[previous_finger] < start) && (start < node.Finger[previous_finger] + powerof(2,node.M)))  {
			node.Finger[start] = node.Finger[previous_finger]
		} else {

			//ask the bootstrap for the successor
			//time.Sleep(time.Second * 1)
			
			status(node, "now finding finger entry for " + strconv.Itoa(i) + " by asking find_successor")
			encoder, decoder := createConnection(node.Bootstrap)
			m := Request{"find_successor", start}
			encoder.Encode(m)
			
			res := new(Response)
			decoder.Decode(&res)
			finger_value := int(res.Result.(float64))

			node.Finger[start] = finger_value
		}
		
	}


}

func update_others(node *ChordNode)(){
	
	for i := 1; i <= node.M; i++ {
		
		//find a new node that may need its finger table updated to the newly joining node
		//func mod is defined as a helper method because golang's % is broken for negative numbers
		incoming_finger := mod((node.Identifier - powerof(2, i-1)),powerof(2,node.M))
		
	
		status(node, "about to look for the predecessor")

		encoder, decoder := createConnection(node.Identifier)
		m := Request{"find_predecessor", incoming_finger}
		encoder.Encode(m)
		
		res := new(Response)
		decoder.Decode(&res)
		p := int(res.Result.(float64))

		//p should be 0...
		status(node, "update_others_find_predecessor result ")
		fmt.Println(res.Result, i)
		
		
		//Now adjust the fingers on node p
		//sending array of [n,i] for p to update its table

		arr := []interface{}{node.Identifier, i}
		
		encoder, decoder = createConnection(p)
		m = Request{"update_finger_table", arr}
		encoder.Encode(m)
		
		res = new(Response)
		decoder.Decode(&res)
		
	}
	
}



//////
// Handle incoming requests for RPCs
//////


func handleConnection(node *ChordNode, conn net.Conn){
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	req := new(Request)
	decoder.Decode(&req)

	//status(node, req.Method)
	
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
	case "update_finger_table" :
		update_finger_table(node, req, encoder)

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

	preform_join := false

	if node.Identifier != node.Bootstrap {
		preform_join = true
	} else {
		//The node is the only one; init finger table, successor, predecessor
		for i := 0; i < node.M; i++ {
			node.Finger[node.Identifier + powerof(2,i) % powerof(2,node.M)] = node.Identifier 
		}
		//finger[0]
		node.Successor = node.Finger[node.Identifier + powerof(2,0) % powerof(2,node.M)]
		node.Predecessor = node.Identifier
	}

	status(node, "About to start the listener")

	//Race conditions if multiple nodes are joining at once??

	networkaddress := config.IpAddress + ":" + config.Port
	ln, err := net.Listen(config.Protocol, networkaddress)
	if err != nil {
		// handle error
	}
	for {
		if preform_join {
			preform_join = false
			go join(node)
		}

		conn, err := ln.Accept() // this blocks until connection or error
		if err != nil {
			// handle error
			continue
		}
		go handleConnection(node, conn) 
	}
	
}
