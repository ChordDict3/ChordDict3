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
/*
//to handle all RPCs between Chord nodes
func SendRPC(identifier string, serviceMethod string, args interface{}, reply interface{}) error {
	client, err := rpc.DialHTTP("tcp", identifier)
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer client.Close()
	err = client.Call(serviceMethod, args, &reply)
	if err != nil {
		log.Fatal("call error:", err)
	}
	return nil
}
*/

//create connection to other nodes
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
	myIdentifier := generateNodeHash(configuration.IpAddress + ":" + configuration.Port, configuration.M)
	
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

func inChordRange(target uint64, start uint64, end uint64, m uint64)(result bool){

	//target 5
	//start 3
	//end 0
	
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

//CHORD FUNCTIONS
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
	m := Response{nil, nil}
	encoder.Encode(m)
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

func (c ChordNode) create() error {
	node.Predecessor = ""
	node.Successor = node.Identifier
	return nil
}

//join an existing chord ring containing node with identifier
func join(identifier string) {
	fmt.Println("entered join")
	node.Predecessor = ""
	//reply := ""

	encoder, decoder := createConnection(identifier)
	m := Request{"find_successor", node.Identifier}
	encoder.Encode(m)
	res := new(Response)
	decoder.Decode(&res)
	node.Successor = res.Result.(string)
	//TODO: ask successor for all DICT3 data we should take from him
}

//find the immediate successor of node with given identifier
func find_successor(req *Request, encoder *json.Encoder) {
	identifier := req.Params.(string)
	fmt.Println("in find_successor: identifier is ", identifier)
	//res := new(Response)
	fmt.Println("hash is ", generateNodeHash(identifier,node.M))
	//probably shouldn't use node.predecessor; not set on create - requires stabilize to have been run to work
/*	if (inChordRange(generateNodeHash(identifier, node.M), generateNodeHash(node.Predecessor, node.M), generateNodeHash(node.Identifier, node.M), node.M)) {
		res := Response{node.Identifier, nil}
		encoder.Encode(res)
	} else */

	if(node.Identifier == node.Successor) {
		res := Response{node.Identifier, nil}
		encoder.Encode(res)
	} else if (inChordRange(generateNodeHash(identifier, node.M), generateNodeHash(node.Identifier, node.M), generateNodeHash(node.Successor, node.M), node.M)) {
		res := Response{node.Successor, nil}
		encoder.Encode(res)
	} else {
		//find closest finger and forward request there; for now just forward around ring
		encoder2, decoder2 := createConnection(node.Successor)
		m := Request{"find_successor", identifier}
		encoder2.Encode(m)
		res := new(Response)
		decoder2.Decode(&res)
		encoder.Encode(res) 

	}
	//encoder.Encode(res)
}

//runs periodically. verifies immediate successor and tells successor about itself
func (c ChordNode) stabilize() error {
	reply := ""
	//SendRPC(node.Successor, "ChordNode.getPredecessor", "", &reply)
	fmt.Println("successors predecessor is ", reply)
	return nil
}

//node thinks it should be the predecessor
func (c ChordNode) notify(identifier string) error {
	if (node.Predecessor == "" || (node.HashID < generateNodeHash(identifier, node.M) && generateNodeHash(identifier, node.M) < generateNodeHash(node.Predecessor, node.M))) {
		node.Predecessor = identifier
	}
	return nil
}

func (c ChordNode) getPredecessor(nothing string, reply *string) error {
	*reply = node.Predecessor
	return nil
}
//func fix_fingers()
//func check_predecessor()

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

func createConnection(node NodeInfo) (encoder *json.Encoder, decoder *json.Decoder){
	conn, err := net.Dial("tcp", node.IpAddress + ":" + node.Port)
	if err != nil {
		fmt.Println("Connection error", err)
	}
	e := json.NewEncoder(conn)
	d := json.NewDecoder(conn)
	
	return e, d
}

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

	case "find_successor" :
		find_successor(req, encoder)
	//case "find_predecessor" :
	//	find_predecessor(req, encoder)
	case "get_successor" :
		get_successor(req, encoder)
	case "get_predecessor" :
		get_predecessor(req, encoder)
	case "set_successor" :
		set_successor(req, encoder)
	case "set_predecessor" :
		set_predecessor(req, encoder)
/*ase "update_finger_table" :
		update_finger_table(node, req, encoder)
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
	
    //Listen for a connection
    for {
    	conn, err := l.Accept() // this blocks until connection or error
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		}
		go handleConnection(node, conn)
    }
    
  //  node.stabilize()
}
