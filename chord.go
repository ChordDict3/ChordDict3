//resource: https://cit.dixie.edu/cs/3410/asst_chord.html - from another CS course;
//			spec for a similar chord project with helpful suggestions

package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"reflect"
	"time"
	"math"
	"io/ioutil"
	"hash/crc64"
	"encoding/json"
	"github.com/HouzuoGuo/tiedot/db"
)

type NodeInfo struct{	// contains hash and connection info
	Identifier uint64
	IpAddress, Port string
}

type ChordNode struct{
	Me, Successor, Predecessor NodeInfo
	FingerTable []NodeInfo
	M uint64 
	Dict3 *db.Col
	//Keys []uint64
}

type Request struct{
	Method string `json:"method"` 
	Params interface{} `json:"params"`
}

type Response struct{
	Result interface{} `json:"result"`
	Error interface{} `json:"error"`
}

type Configuration struct{
	ServerID string `json:"serverID"`
	Protocol string `json:"protocol"`
	IpAddress string `json:"ipAddress"`
	Port string `json:"port"`
	M uint64 `json:"M"`
	PersistentStorageContainer struct {
		File string `json:"file"`
	} `json:"persistentStorageContainer"`
	Methods []string `json:"methods"`
}

type DictValue struct {
	Content interface{}
	Size float64 
	Created time.Time
	Modified time.Time
	Accessed time.Time
	Permission string
}

//to handle all RPCs between Chord nodes
func (c ChordNode) SendRPC(identifier string, serviceMethod string, args interface{}, reply interface{}) {
	client, _ := rpc.Dial("tcp", identifier)
	defer client.Close()
	client.Call(serviceMethod, args, reply)
}

//generates sha1 hash of "ip:port" string; sha1 too large for int; using big.Int
//returning hash mod 2^m as the hash ID for the node
//still returning *big.Int; want int - needs work
func generateNodeHash(identifier string, m uint64) uint64 {
	hasher := crc64.New(crc64.MakeTable(123456789)) //any number will do
	hasher.Write([]byte(identifier))
	hash64 := hasher.Sum64()
	hash := hash64 % uint64(1 << m)
	return hash 
}

func generateKeyRelHash(key string, rel string, m uint64) uint64 {
	keyHash := generateNodeHash(key, m)
	relHash := generateNodeHash(rel, m)
	shiftOffset := uint64(0)
	if (m % 2 != 0) {  // if m is odd, additional shift needed
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

func makeDictValue(content interface{}, permission string) DictValue {
	now := time.Now()
	dictVal := DictValue{
		Content: content,
		Size: float64(reflect.TypeOf(content).Size()),
		Created: now,
		Modified: now,
		Accessed: now,
		Permission: permission,
	}
	return dictVal
}

//CHORD FUNCTIONS
//create a new chord ring
func create(configuration Configuration) *ChordNode{
	myIdentifier := generateNodeHash(configuration.IpAddress + ":" + configuration.Port, (1 << configuration.M))
	
	myDBDir := configuration.PersistentStorageContainer.File
	// (Create if not exist) open a database
	myDB, err := db.OpenDB(myDBDir)
	if err != nil {
		panic(err)
	}
	dbName := "Triplets"+configuration.Port
	if err := myDB.Create(dbName); err != nil {
		panic(err)
	}	
	triplets := myDB.Use(dbName)
	// Create indexes here??
	// TODO: Do not create index if it already exists?
	if err := triplets.Index([]string{"key"}); err != nil {
		panic(err)
	}
	if err := triplets.Index([]string{"rel"}); err != nil {
		panic(err)
	}
	c := ChordNode{
		Me: NodeInfo{Identifier: myIdentifier,
			IpAddress: configuration.IpAddress,
			Port: configuration.Port,
		},
		Successor: NodeInfo{},
		Predecessor: NodeInfo{},
		FingerTable: make([]NodeInfo, 0),
		M: configuration.M,
		Dict3: triplets,
	}
	return &c
}

//join an existing chord ring containing node with identifier
func (c ChordNode) join(ringNode NodeInfo) {
	c.Predecessor = NodeInfo{}
	c.Successor = c.get_successor(c.Me.Identifier)
	fmt.Println(c.Successor)
	//TODO: ask successor for all DICT3 data we should take from him
}

//find the immediate successor of node with given identifier
func (c ChordNode) get_successor(identifier uint64) NodeInfo {
//the node being asked is the successor
//is this smart to do? not setting predecessor on create; requires stabilize to run to work
	if (identifier > c.Predecessor.Identifier) && (identifier <= c.Me.Identifier) {
		return c.Me
	} else if (identifier < c.Successor.Identifier) && (identifier > c.Me.Identifier) {
		return c.Successor
	} else {
		//find closest finger and forward request there
		// but for now just connect to successor
		return c.remote_get_successor(c.Successor, identifier)
	}
}

func (c ChordNode) remote_get_successor(remoteNode NodeInfo, identifier uint64) NodeInfo {
	e, d := createConnection(remoteNode)
	req := Request{Method: "get_successor", Params: identifier}
	e.Encode(req)
	resp := new(Response)
	d.Decode(&resp)
	fmt.Println(resp.Result.(NodeInfo))
	return resp.Result.(NodeInfo)
}

func handle_remote_get_successor(n *ChordNode, req *Request, encoder *json.Encoder) {
	fmt.Println("handle_remote_get_successor")
	identifier := req.Params.(uint64)
	successor := n.get_successor(identifier)
	resp := Response{successor, nil}
	encoder.Encode(resp)
}
//func stabilize()
//func notify(identifier string)
//func fix_fingers()
//func notify
//func check_predecessor()

/////
// RPC Functions
////
/*
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
*/
/*
closest_preceding_finger

if finger[i].node in (n, id):
return finger[i].node
*/
/*
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
/*
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
*/
/////
// Helper Functions
/////

func powerof(x int, y int)(val int){
	val = int(math.Pow(float64(x),float64(y)))
	return val
}

func status(node *ChordNode, msg string){
	fmt.Println("Ident: ", node.Me.Identifier, " P: ", node.Predecessor, " S: ", node.Successor, " " ,  msg)
} 

//Because golang's % of negative numbers is broken
//https://github.com/golang/go/issues/448
func mod(m uint64, n uint64) uint64 {
	return ((m % n) + n) % n
}

/////
// Node setup Functions
/////
func createConnection(node NodeInfo) (encoder *json.Encoder, decoder *json.Decoder){
	conn, err := net.Dial("tcp", node.IpAddress + ":" + node.Port)
	if err != nil {
		fmt.Println("Connection error", err)
	}
	e := json.NewEncoder(conn)
	d := json.NewDecoder(conn)
	
	return e, d
}
/*
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
*/
//////
// Dict functions
//////
func insertValueIntoTriplets(key string, rel string, val map[string]interface{}, triplets *db.Col) {
	_, err := triplets.Insert(map[string]interface{}{
		"key": key,
		"rel": rel,
		"val": val})
	if (err != nil) {
		panic(err)
	}
}

func query_key_rel(key string, rel string, triplets *db.Col) (queryResult map[int]struct{}){

	var query interface{}

	//{"n" means "intersection" of the two queries, logical AND

	json.Unmarshal([]byte(`{"n": [{"eq": "` + key + `", "in": ["key"]}, {"eq": "` + rel + `", "in": ["rel"]}]}`), &query)

	q_result := make(map[int]struct{}) // query result (document IDs) goes into map keys

	if err := db.EvalQuery(query, triplets, &q_result); err != nil {
		panic(err)
	}

	return q_result
}

func lookup(n *ChordNode, req *Request, encoder *json.Encoder) {
	triplets := n.Dict3

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
			m := Response{val, nil}
			encoder.Encode(m)
		}
		
	} else {	// Key/rel not in DB
		//get hash for key/rel
		keyRelHash := generateKeyRelHash(key, rel, n.M)
		//find closest successor/finger node for keyRelHash
		successor := n.get_successor(keyRelHash)
		//forward request to successor
		forwardEncoder, decoder := createConnection(successor)
		forwardEncoder.Encode(req)
		resp := new(Response)
		decoder.Decode(&resp)
		encoder.Encode(resp)
	}
}

func insert(n *ChordNode, req *Request, encoder *json.Encoder, update bool){
	fmt.Println("entering insert")
	triplets := n.Dict3

	p := req.Params
	arr := p.([]interface{})
	
	key := arr[0].(string)
	rel := arr[1].(string)
	val := arr[2]

	keyRelHash := generateKeyRelHash(key, rel, n.M)
	fmt.Printf("keyRelHash: %b\n", keyRelHash)
	// get successor; if Me, then insert into DB, else forward
	successor := n.get_successor(keyRelHash)
	fmt.Println(successor)
	if (successor.Identifier != n.Me.Identifier) {
		forwardEncoder, decoder := createConnection(successor)
		forwardEncoder.Encode(req)
		resp := new(Response)
		decoder.Decode(&resp)
		encoder.Encode(resp)
	} else {	
		// See if there this key/val is already in DB
		queryResult := query_key_rel(key, rel, triplets)
		if len(queryResult) != 0 {
			if update{
				// insertOrUpdate() now replaces the key/rel with an updated value
				for i := range queryResult {
					readBack, err := triplets.Read(i)
					if err != nil {
						panic(err)
					}
				
					dictVal := readBack["val"].(map[string]interface{})
					//update content
					dictVal["Content"] = val
					//update with new Accessed/Modified time
					now := time.Now()
					dictVal["Accessed"] = now
					dictVal["Modified"] = now
					insertValueIntoTriplets(key, rel, dictVal, triplets)
				}
			} else {
				// insert() fails if key/rel already exists
				m := Response{false, nil}
				encoder.Encode(m)
			}
		} else {
			dictVal := makeDictValue(val, "RW")
			_, err := triplets.Insert(map[string]interface{}{
				"key": key,
				"rel": rel,
				"val": dictVal})
			if err != nil {
				panic(err)
			}
			//fmt.Println("Inserting ", docID)
			
			//insertOrUpdate doesn't have a return value
			if update == false {
				m := Response{true, nil}
				encoder.Encode(m)
			}
		}
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

	fmt.Println("deciding method: " + req.Method)
	switch req.Method {
		case "find_successor" :
			handle_remote_get_successor(node, req, encoder)
//		case "find_predecessor" :
//			find_predecessor(node, req, encoder)
//		case "get_successor" :
//			get_successor(node, req, encoder)
//		case "get_predecessor" :
//			get_predecessor(node, req, encoder)
//		case "set_successor" :
//			set_successor(node, req, encoder)
//		case "set_predecessor" :
//			set_predecessor(node, req, encoder)
//		case "update_finger_table" :
//			update_finger_table(node, req, encoder)
		case "lookup" :
			lookup(node, req, encoder)
		case "insert" :
			insert(node, req, encoder, false)
	}
}

func readConfig()(config *Configuration){

	dat, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		//fmt.Println("Error reading file")
		panic(err)
	}

	b_arr := []byte(string(dat))

	config = new(Configuration)
	if err := json.Unmarshal(b_arr, &config); err != nil {
		panic(err)
	}

	//fmt.Printf("Parsed : %+v", config)

	return config
}

func main() {
	// Parse argument configuration block
	config := *readConfig()
	fmt.Println("creating node...")
	node := create(config)
	fmt.Printf("node identifier: %b\n", node.Me.Identifier)

	if(len(os.Args) > 2) {
		ringNodeIpAddress := os.Args[2]
		ringNodePort := os.Args[3]
		address := ringNodeIpAddress + ":" + ringNodePort
		ringNode := NodeInfo{generateNodeHash(address, node.M), ringNodeIpAddress, ringNodePort}
		node.join(ringNode)
	}

	networkaddress := config.IpAddress + ":" + config.Port
	ln, err := net.Listen("tcp", networkaddress)
	if err != nil {
		panic(err)
	}
	for {
		fmt.Println("listening...")
		conn, err := ln.Accept() // this blocks until connection or error
		if err != nil {
			panic(err)	
		}
		go handleConnection(node, conn) 
	}
}
