//resource: https://cit.dixie.edu/cs/3410/asst_chord.html - from another CS course;
//			spec for a similar chord project with helpful suggestions

package main

import (
	"fmt"
//	"net/rpc"
	"net"
//	"net/http"
	"log"
	"encoding/json"
	"os"
	"strconv"
	"math"
	//"crypto/sha1"
	"hash/crc64"
	"time"
)

type ChordNode struct{
	Identifier string
	IP string
	Port int
	HashID uint64
	Successor string
	Predecessor string
	Fingers []string
	M uint64 
	Keys []uint64
}

var node = new(ChordNode) //made global so accessible by chord functions

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
	Port int
}

//////////////////
//HELPER FUNCTIONS
//////////////////
func readTestConfig()(config *TestConfiguration){
	config = new(TestConfiguration)
	config.IpAddress = "127.0.0.1"
	config.Protocol = "tcp"

	return config
}

//for RPCs
func createConnection(identifier string) (encoder *json.Encoder, decoder *json.Decoder){
	fmt.Println("Creating connection to ", identifier)
	conn, err := net.Dial("tcp", identifier)
	if err != nil {
		fmt.Println("Connection error", err)
	}
	e := json.NewEncoder(conn)
	d := json.NewDecoder(conn)
	
	return e, d
}

//generates sha1 hash of "ip:port" string; sha1 too large for int; using big.Int
//returning hash mod 2^m as the hash ID for the node
func generateNodeHash(identifier string, m uint64) uint64 {
	hasher := crc64.New(crc64.MakeTable(123456789)) //any number will do
	hasher.Write([]byte(identifier))
	hash64 := hasher.Sum64()
	hash := mod(hash64, uint64(1 << m))
	return hash 
}

func generateKeyRelHash(key string, rel string, m uint64) uint64 {
	keyHash := generateNodeHash(key, m)
	relHash := generateNodeHash(rel, m)
	shiftOffset := uint64(0)
	if (mod(m, uint64(2)) != 0) {  // if m is odd, additional shift needed
		shiftOffset = uint64(1)
	}
		
	upper := uint64( ((1 << m) - 1) ^ ((1 << (m/2)) - 1) )
	lower := uint64( ((1 << m) - 1) ^ ( ((1 << (m/2 + shiftOffset)) - 1) << (m/2)) )
	fmt.Printf("key  :%0b\n", keyHash)
	fmt.Printf("rel  :%0b\n", relHash)
	fmt.Printf("upper:%0b\n", upper)
	fmt.Printf("lower:%0b\n", lower)
	fmt.Printf("keyup:%0b\n", keyHash&upper)
	fmt.Printf("rello:%0b\n", relHash&lower)
	hash := (keyHash & upper) | (relHash & lower)
	return hash
}

//Determines if target hash is in a slice of the chord ring
func inChordRange(target uint64, start uint64, end uint64, m uint64)(result bool){
	//0 6 1
	if target > start && target <= end {
		return true
	}
	if end < start {
		//wrap around case, target is not wrapped around
		if target > start && target <= end + powerof(2, m) {
			return true
		} 
	}
	if (end < start) && (target <= end) {
		//wrap around case, target is wrapped around
		if (target + powerof(2, m) > start) && (target <= end) {
			return true
		}
	}
	return false
}

func powerof(x uint64, y uint64)(val uint64){
	val = uint64(math.Pow(float64(x),float64(y)))
	return val
}

//Because golang's % of negative numbers is broken
//https://github.com/golang/go/issues/448
func mod(m uint64, n uint64) uint64 {
	return ((m % n) + n) % n
}

/*
func status(node *ChordNode, msg string){
	fmt.Println("Ident: ", node.Identifier, " P: ", node.Predecessor, " S: ", node.Successor, " F: ", node.Finger, " ",  msg)

} 
*/

/////////////////
//CHORD FUNCTIONS
/////////////////
//create a new chord ring. set predecessor to nil and successor to self
func get_successor(req *Request, encoder *json.Encoder){
	m := Response{node.Successor, nil}
	encoder.Encode(m)
	return

}

func get_predecessor(req *Request, encoder *json.Encoder){
	m := Response{node.Predecessor, nil}
	encoder.Encode(m)
	return
}

func set_predecessor(req *Request, encoder *json.Encoder){
	id := req.Params.(string)
	node.Predecessor = id
//	m := Response{nil, nil}
//	encoder.Encode(m)
	return
}

func set_successor(req *Request, encoder *json.Encoder){
	id := req.Params.(string)
	node.Successor = id
	
	//status(node, "Successor updated")
		
	m := Response{nil, nil}
	encoder.Encode(m)
	return
}

func transfer_keys_on_shutdown(req *Request, encoder *json.Encoder) {    
    fmt.Println("entered transfer_keys_on_shutdown")
    forwarded_keys := req.Params.([]uint64)
    
    for _, value := range forwarded_keys {
        node.Keys = append(node.Keys, value)
    }
    
    m := Response("Success", nil)
    encoder.Encode(m)
    return
}

func transfer_keys_on_join(req *Request, encoder *json.Encoder) {
    // grab successor keys
    // loop through successor keys and check if key# <= node#
    // add those keys to node's Key list
}

func (c ChordNode) create() error {
	node.Predecessor = ""
	node.Successor = node.Identifier
	return nil
}

//join an existing chord ring containing node with identifier
func join(identifier string) {
	fmt.Println("entered join")
	node.Predecessor = ""

	encoder, decoder := createConnection(identifier)
	m := Request{"find_successor", node.Identifier}
	encoder.Encode(m)
	res := new(Response)
	decoder.Decode(&res)
	node.Successor = res.Result.(string)

	//TODO: ask successor for all DICT3 data we should take from him
    m = Request{"transfer_keys_on_join", identifier}
    encoder.Encode(m)
    res := new(Response)
    decoder.Decode(&res)
    
    returned_keys := res.Result.([]uint64)
    for _, value := range returned_keys {
        node.Keys = append(node.Keys, value)
    }
}

//find the immediate successor of node with given identifier
func find_successor(req *Request, encoder *json.Encoder) {
	identifier := req.Params.(string)

	if(node.Identifier == node.Successor) {
		res := Response{node.Identifier, nil}
		encoder.Encode(res)
	} else if (inChordRange(generateNodeHash(identifier, node.M), generateNodeHash(node.Identifier, node.M), generateNodeHash(node.Successor, node.M), node.M)) {
		res := Response{node.Successor, nil}
		encoder.Encode(res)
	} else {
		//find closest finger and forward request there; for now just forward around ring
		//encoder2, decoder2 := createConnection(node.Successor) //change to closest finger
		closest_preceding_node := closest_preceding_node(generateNodeHash(identifier, node.M), node.M)
		encoder2, decoder2 := createConnection(closest_preceding_node)
		m := Request{"find_successor", identifier}
		encoder2.Encode(m)
		res := new(Response)
		decoder2.Decode(&res)
		encoder.Encode(res) 
	}
}

//runs periodically:
//verifies immediate successor and tells successor about itself
//needs to also verify predecessor and tell predecessor about itself
func stabilize() {
	//verify immediate successor; tell successor about itself
	encoder, decoder := createConnection(node.Successor)
	m := Request{"get_predecessor", ""}
	encoder.Encode(m)
	res := new(Response)
	decoder.Decode(&res)
	successors_predecessor := res.Result.(string)
	if (successors_predecessor == "") {
		//successor has no predecesor; we should be the predecessor - there are only 2 nodes
		m = Request{"set_predecessor", node.Identifier}
		encoder.Encode(m)
	} else if (node.Successor == node.Identifier) {
		//successor is set to myself; 2 nodes and this was first node
		node.Successor = successors_predecessor
	}else if(inChordRange(generateNodeHash(successors_predecessor, node.M), generateNodeHash(node.Identifier, node.M), generateNodeHash(node.Successor, node.M), node.M)) {
		fmt.Println("stabilize: in chord range")
		node.Successor = successors_predecessor		
	} 
	if(node.Identifier != node.Successor) {	
		fmt.Println("going to notify ", node.Successor, "that ", node.Identifier, "should be his predecesor")	
		encoder, decoder = createConnection(node.Successor)
		m = Request{"notify", node.Identifier}
		encoder.Encode(m)
	} else if (inChordRange(generateNodeHash(node.Identifier, node.M), generateNodeHash(successors_predecessor, node.M), generateNodeHash(node.Successor, node.M), node.M)) {
		// me < succ_pred < succ; successor's predecessor should be my successor
		if(node.Identifier != node.Successor) {	
			fmt.Println("going to notify ", node.Successor, "that ", node.Identifier, "should be his predecesor")	
			encoder, decoder = createConnection(node.Successor)
			m = Request{"notify", node.Identifier}
			encoder.Encode(m)
		}
	}	
}

//check if identifier should be this node's predecessor
func notify(req *Request, encoder *json.Encoder) {
	identifier := req.Params.(string)
	if (node.Predecessor == "") { 
	//node has no predecessor, set identifier as node's predecessor
		node.Predecessor = identifier
		//res := Response{identifier, nil}
		//encoder.Encode(res)
	} else if (inChordRange(generateNodeHash(identifier, node.M), generateNodeHash(node.Predecessor, node.M), generateNodeHash(node.Identifier, node.M), node.M)) { 
	//predecessor < identifier < node; ID should be pred
		node.Predecessor = identifier
		//res := Response{identifier, nil}
		//encoder.Encode(res)
	} else { 
	//node's predecessor was already correct
		//res := Response{node.Predecessor, nil}
		//encoder.Encode(res)
	}
}

func successor_of_hash(req *Request, encoder *json.Encoder) {
	fmt.Println("in succ of hash")
	hash := uint64(req.Params.(float64))
	retval := ""
	if (hash == generateNodeHash(node.Identifier, node.M)) {
		retval = node.Identifier
		fmt.Println("successor of hash is: ", retval)
		res := Response{retval, nil}
		encoder.Encode(res)
	} else if (inChordRange(hash, generateNodeHash(node.Identifier, node.M), generateNodeHash(node.Successor, node.M), node.M)) {
		retval = node.Successor
		fmt.Println("successor of hash is: ", retval)
		res := Response{retval, nil}
		encoder.Encode(res)
	} else {
		//encoder2, decoder2 := createConnection(node.Successor) //should change this to closest finger
		closest_preceding_node := closest_preceding_node(hash, node.M)
		encoder2, decoder2 := createConnection(closest_preceding_node)
		m := Request{"successor_of_hash", hash}
		encoder2.Encode(m)
		res := new(Response)
		decoder2.Decode(&res)
		fmt.Println("successor of hash is: ", res.Result.(string))
		encoder.Encode(res)
	}

}

//runs periodically; refreshes finger table entries
//next stores the index of the next finger to fix
func fix_fingers() {
	finger_value := ""
	fmt.Println("fix fingers")
	index := 0
	for i := uint64(0); i < node.M; i++ {
		start := uint64((node.HashID + powerof(2,i)) % powerof(2,node.M))
		if(inChordRange(start, generateNodeHash(node.Identifier, node.M), generateNodeHash(node.Successor, node.M), node.M)) {
			finger_value = node.Successor
		} else if(inChordRange(start, generateNodeHash(node.Predecessor, node.M), generateNodeHash(node.Identifier, node.M), node.M)) {
			finger_value = node.Identifier
		} else if (node.Identifier != node.Successor) {
			encoder, decoder := createConnection(node.Successor)
			m := Request{"successor_of_hash", start}
			encoder.Encode(m)
			res := new(Response)
			decoder.Decode(&res)
			finger_value = res.Result.(string)
		} else {
			finger_value = node.Identifier
		}
		node.Fingers[index] = finger_value
		index++
	}
}

func closest_preceding_node(hash uint64, m uint64) string {
	for i := uint64(1); i < node.M; i++ {
		if(node.Fingers[i-1] != "" && node.Fingers[i] != "") {
			if(inChordRange(hash, generateNodeHash(node.Fingers[i-1], node.M), generateNodeHash(node.Fingers[i], node.M), node.M)) {
				return node.Fingers[i-1]
			}
		}
	}
	//no finger was closest; return self
	return node.Identifier
}

//not relevant; won't have node failures. can be set on node shutdown
//func check_predecessor()


/*
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

//////
// Dict functions
//////

func lookup(node *ChordNode, req *Request, encoder *json.Encoder, triplets *db.Col) {
	p := req.Params
	arr := p.([]interface{})
	
	key := arr[0].(string)
	rel := arr[1].(string)

	// See if there this key/val is already in DB
	queryResult := query_key_rel(key, rel, triplets)
	if len(queryResult) != 0 {
		for i := range queryResult {
			readBack, err := triplets.Read(i)
			if err != nil {
				panic(err)
			}
			
			val := readBack["val"].(map[string]interface{})
			fmt.Println(val)
			//update with new Accessed time
			val["Accessed"] = time.Now()
			insertValueIntoTriplets(key, rel, val, triplets)
			//send response
			m := Response{val, id, nil}
			encoder.Encode(m)
		}
		
	} else {
		// Key/rel not in DB
		m := Response{nil, id, nil}
		encoder.Encode(m)
	}
}

func shutdown(node *ChordNode, encoder *json.Encoder) {
    // Get the node's hash ID
    hashID uint64 = node.HashID
    // Set the hash ID to something unreachable
    node.HashID = -1
    
    // Get the node's predecessor
    predecessor string := node.Predecessor
    // Set the node's predecessor to nothing
    node.Predecessor = ""
    
    // Get the node's successor
    successor string := node.Successor
    // Set the node's successor to nothing
    node.Successor = ""
    
    // If this is the only node in the ring
    if (successor == predecessor && successor == node.Identifier) {
        // TODO: dump the DB
    } else {
        // Copy all keys from node to successor
        successor_encoder, successor_decoder := createConnection(successor)
        m := Request{"transfer_keys_on_shutdown", node.Keys}
        successor_encoder.Encode(m)
        res := new(Response)
        decoder.Decode(&res)
    }
    
	node.Keys = {}
    os.Exit(0)
    client_message := Response{nil, nil}
    encoder.Encode(client_message)
}

//////
// Handle incoming requests for RPCs
//////

*/
func handleConnection(node *ChordNode, conn net.Conn){
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	req := new(Request)
	decoder.Decode(&req)

	//status(node, req.Method)
	
	switch req.Method {

	case "find_successor" :
		find_successor(req, encoder)
	case "get_successor" :
		get_successor(req, encoder)
	case "get_predecessor" :
		get_predecessor(req, encoder)
	case "set_successor" :
		set_successor(req, encoder)
	case "set_predecessor" :
		set_predecessor(req, encoder)
	case "successor_of_hash" :
		successor_of_hash(req, encoder)
	case "notify" :
		notify(req, encoder)
/*
	case "lookup" :
		lookup(node, req, encoder, triplets)*/
	}
}


func main() {
	// Parse argument configuration block
	//node := new(ChordNode)
	config := readTestConfig()

	// For early development, the chord node listens on port 1000x, where x is its identifier
	// The identifier is passed as the first command line argument
	// The bootstrap (another node to use for join()) is the second optional argument
	portString := "1000"
	portString += os.Args[1]
	config.Port, _ = strconv.Atoi(portString) 
	config.ServerID = config.IpAddress
	config.ServerID += ":"
	config.ServerID += portString

	//Initialize node fields
	node.Identifier = config.ServerID
	node.IP = config.IpAddress
	node.Port = config.Port
	node.M = 3
	node.HashID = generateNodeHash(node.Identifier, node.M) 
	node.Successor = ""
	node.Predecessor = ""
	node.Fingers = make([]string, node.M)
    node.Keys = {}
	fmt.Println("Initialized!")

	if len(os.Args) > 2 {
		ipportString := "127.0.0.1:1000"
		ipportString += os.Args[2]
		join(ipportString)
	} else {
		fmt.Println("Creating a new chord ring...")
		node.create()
	}

	fmt.Println("node identifier: "+node.Identifier)
	fmt.Println("node successor: "+node.Successor)
	fmt.Println("node hashID: ", node.HashID)

	fmt.Println("Setting up a listener...")
	
	l, err := net.Listen(config.Protocol, node.Identifier)
	if err != nil {
		log.Fatal("Listen error:", err)
	}

	fmt.Println("Listening on ", node.Identifier)	
	

	//ticker for stabilize
	ticker := time.NewTicker(time.Millisecond * 5000)
	go func() {
		for t := range ticker.C {
			stabilize()
			fmt.Println("node identifier: "+node.Identifier)
			fmt.Println("node successor: "+node.Successor)
			fmt.Println("node predecessor: "+node.Predecessor)
			fmt.Println("node hashID: ", node.HashID)

			_ = t
		}
	}()

		ticker2 := time.NewTicker(time.Millisecond * 20000)
	go func() {
		for t2 := range ticker2.C {
			fix_fingers()
			fmt.Println("fingers[0]: ", node.Fingers[0])
			fmt.Println("fingers[1]: ", node.Fingers[1])
			fmt.Println("fingers[2]: ", node.Fingers[2])

			_ = t2
		}
	}()

    //Listen for a connection
    for {
    	conn, err := l.Accept() // this blocks until connection or error
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		}
		go handleConnection(node, conn)
    }
}

/*
func main() {
	m := uint64(9)
	fmt.Printf("%b\n", generateNodeHash("127.0.0.1:1000", m))
	fmt.Printf("%b\n", generateKeyRelHash("key", "rel", m))
	fmt.Printf("%b\n", generateKeyRelHash("ney", "rel", m))
	fmt.Printf("%b\n", generateKeyRelHash("xey", "rel", m))
	fmt.Printf("%b\n", generateKeyRelHash("rey", "rgl", m))
	fmt.Printf("%b\n", generateKeyRelHash("wey", "rfl", m))
	fmt.Printf("%b\n", generateKeyRelHash("sey", "rel", m))
	fmt.Printf("%b\n", generateKeyRelHash("oey", "rdl", m))
	fmt.Printf("%b\n", generateKeyRelHash("pey", "rcl", m))
	fmt.Printf("%b\n", generateKeyRelHash(".ey", "rbl", m))
	fmt.Printf("%b\n", generateKeyRelHash("vey", "ral", m))
}
*/