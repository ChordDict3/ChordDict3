//resource: https://cit.dixie.edu/cs/3410/asst_chord.html - from another CS course;
//			spec for a similar chord project with helpful suggestions

package main

import (
	"fmt"
	"net"
	"log"
	"os"
	"reflect"
	"time"
	"math"
	"io/ioutil"
	"hash/crc64"
	"encoding/json"
	"github.com/HouzuoGuo/tiedot/db"
	"strconv"
)


type ChordNode struct{
	Me string
	IP string
	Port int
	HashID uint64
	Successor string
	Predecessor string
	Fingers []string
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
	Created int64
	Modified int64
	Accessed int64
	Permission string
}

//create connection to other nodes
func createConnection(netAddr string) (encoder *json.Encoder, decoder *json.Decoder){
	fmt.Println("Creating connection to ", netAddr)
	conn, err := net.Dial("tcp", netAddr)

	if err != nil {
		fmt.Println("Connection error", err)
	}
	e := json.NewEncoder(conn)
	d := json.NewDecoder(conn)
	
	return e, d
}

//generates sha1 hash of "ip:port" string; sha1 too large for int; using big.Int
//returning hash mod 2^m as the hash ID for the node
func generateNodeHash(netAddr string, m uint64) uint64 {
	hasher := crc64.New(crc64.MakeTable(123456789)) //any number will do
	hasher.Write([]byte(netAddr))
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

func generateKeyPartialHashes(key string, m uint64) []uint64 {
	keyHash := generateNodeHash(key, m)
	upper := uint64( ((1 << m) - 1) ^ ((1 << (m/2)) - 1) )
	
	partialHashes := make([]uint64, (1 << (m/2)))
	for i := 0; i < (1 << (m/2)); i++ {
		partialHashes[i] = (keyHash & upper) | uint64(i)
	}
	return partialHashes
}

func generateRelPartialHashes(rel string, m uint64) []uint64 {
	relHash := generateNodeHash(rel, m)
	shiftOffset := uint64(0)
	if (m % 2 != 0) {  // if m is odd, additional shift needed
		shiftOffset = uint64(1)
	}
	lower := uint64( ((1 << m) - 1) ^ ( ((1 << (m/2 + shiftOffset)) - 1) << (m/2)) )
	
	partialHashes := make([]uint64, (1 << ((m/2) + shiftOffset)))
	for i := 0; i < (1 << ((m/2) + shiftOffset)); i++ {
		partialHashes[i] = (uint64(i) << (m/2)) | (relHash & lower)
	}
	return partialHashes
}

func makeDictValue(content interface{}, permission string) DictValue {
	now := time.Now().Unix()
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
func create(config Configuration) *ChordNode{
	netAddr := config.IpAddress + ":" + config.Port
	hash := generateNodeHash(netAddr, config.M)
	
	myDBDir := config.PersistentStorageContainer.File
	// (Create if not exist) open a database
	myDB, err := db.OpenDB(myDBDir)
	if err != nil {
		panic(err)
	}
	dbName := "Triplets"+config.Port
	if err := myDB.Create(dbName); err != nil {
		//ignoring this error?
		//panic(err)
	}	
	triplets := myDB.Use(dbName)
	// Create indexes here??
	// TODO: Do not create index if it already exists?
	if err := triplets.Index([]string{"key"}); err != nil {
		//ignoring?
		//panic(err)
	}
	if err := triplets.Index([]string{"rel"}); err != nil {
		//ignoring?
		//panic(err)
	}
	c := ChordNode{
		Me: netAddr,
		Successor: "",
		Predecessor: "",
		Fingers: make([]string, 3),
		HashID: hash,
		M: config.M,
		Dict3: triplets,
	}
	return &c
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

/////////////////
//CHORD FUNCTIONS
/////////////////
//create a new chord ring. set predecessor to nil and successor to self
func (node *ChordNode)get_successor(req *Request, encoder *json.Encoder){
	m := Response{node.Successor, nil}
	encoder.Encode(m)
	return

}

func (node *ChordNode)get_predecessor(req *Request, encoder *json.Encoder){
	m := Response{node.Predecessor, nil}
	encoder.Encode(m)
	return
}

func (node *ChordNode)set_predecessor(req *Request, encoder *json.Encoder){
	id := req.Params.(string)
	node.Predecessor = id
//	m := Response{nil, nil}
//	encoder.Encode(m)
	return
}

func (node *ChordNode)set_successor(req *Request, encoder *json.Encoder){
	id := req.Params.(string)
	node.Successor = id
	
	//status(node, "Successor updated")
		
	m := Response{nil, nil}
	encoder.Encode(m)
	return
}

func (node *ChordNode)get_successor_of_hash(hash uint64) string {
	if (inChordRange(hash, generateNodeHash(node.Predecessor, node.M), generateNodeHash(node.Me, node.M), node.M)) {
		return node.Me
	} else if (inChordRange(hash, generateNodeHash(node.Me, node.M), generateNodeHash(node.Successor, node.M), node.M)) {
		return node.Successor
	} else {
		return node.Successor
	}
}

func (node *ChordNode) create() error {
	node.Predecessor = ""
	node.Successor = node.Me
	return nil
}

//join an existing chord ring containing node with identifier
func (node *ChordNode)join(netAddr string) {
	fmt.Println("entered join")
	node.Predecessor = ""

	encoder, decoder := createConnection(netAddr)
	m := Request{"find_successor", node.Me}
	encoder.Encode(m)
	res := new(Response)
	decoder.Decode(&res)
	node.Successor = res.Result.(string)
	//TODO: ask successor for all DICT3 data we should take from him
}

//find the immediate successor of node with given identifier
func (node *ChordNode)find_successor(req *Request, encoder *json.Encoder) {
	netAddr := req.Params.(string)

	if(node.Me == node.Successor) {
		res := Response{node.Me, nil}
		encoder.Encode(res)
	} else if (inChordRange(generateNodeHash(netAddr, node.M), generateNodeHash(node.Me, node.M), generateNodeHash(node.Successor, node.M), node.M)) {
		res := Response{node.Successor, nil}
		encoder.Encode(res)
	} else {
		//find closest finger and forward request there; for now just forward around ring
		encoder2, decoder2 := createConnection(node.Successor)
		m := Request{"successor_of_hash", netAddr}
		encoder2.Encode(m)
		res := new(Response)
		decoder2.Decode(&res)
		encoder.Encode(res) 
	}
}

//runs periodically:
//verifies immediate successor and tells successor about itself
//needs to also verify predecessor and tell predecessor about itself
func (node *ChordNode)stabilize() {
	//verify immediate successor; tell successor about itself
	encoder, decoder := createConnection(node.Successor)
	m := Request{"get_predecessor", ""}
	encoder.Encode(m)
	res := new(Response)
	decoder.Decode(&res)
	successors_predecessor := res.Result.(string)
	if (successors_predecessor == "") {
		//successor has no predecesor; we should be the predecessor - there are only 2 nodes
		m = Request{"set_predecessor", node.Me}
		encoder.Encode(m)
	} else if (node.Successor == node.Me) {
		//successor is set to myself; 2 nodes and this was first node
		node.Successor = successors_predecessor
	}else if(inChordRange(generateNodeHash(successors_predecessor, node.M), generateNodeHash(node.Me, node.M), generateNodeHash(node.Successor, node.M), node.M)) {
		fmt.Println("stabilize: in chord range")
		node.Successor = successors_predecessor		
	} 
	if(node.Me != node.Successor) {	
		fmt.Println("going to notify ", node.Successor, "that ", node.Me, "should be his predecesor")	
		encoder, decoder = createConnection(node.Successor)
		m = Request{"notify", node.Me}
		encoder.Encode(m)
	} else if (inChordRange(generateNodeHash(node.Me, node.M), generateNodeHash(successors_predecessor, node.M), generateNodeHash(node.Successor, node.M), node.M)) {
		// me < succ_pred < succ; successor's predecessor should be my successor
		if(node.Me != node.Successor) {	
			fmt.Println("going to notify ", node.Successor, "that ", node.Me, "should be his predecesor")	
			encoder, decoder = createConnection(node.Successor)
			m = Request{"notify", node.Me}
			encoder.Encode(m)
		}
	}	
}

//check if identifier should be this node's predecessor
func (node *ChordNode)notify(req *Request, encoder *json.Encoder) {
	identifier := req.Params.(string)
	if (node.Predecessor == "") { 
	//node has no predecessor, set identifier as node's predecessor
		node.Predecessor = identifier
		//res := Response{identifier, nil}
		//encoder.Encode(res)
	} else if (inChordRange(generateNodeHash(identifier, node.M), generateNodeHash(node.Predecessor, node.M), generateNodeHash(node.Me, node.M), node.M)) { 
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

func updateTripletValue(docId int, key string, rel string, val map[string]interface{}, triplets *db.Col) {
	err := triplets.Update(docId, map[string]interface{} {
		"key": key,
		"rel": rel,
		"val": val})
	if (err != nil) {
		panic(err)
	}
}

func (node *ChordNode)successor_of_hash(req *Request, encoder *json.Encoder) {
	fmt.Println("in succ of hash")
	hash := uint64(req.Params.(float64))
	retval := ""
	if (hash == generateNodeHash(node.Me, node.M)) {
		retval = node.Me
		fmt.Println("successor of hash is: ", retval)
		res := Response{retval, nil}
		encoder.Encode(res)
	} else if (inChordRange(hash, generateNodeHash(node.Me, node.M), generateNodeHash(node.Successor, node.M), node.M)) {
		retval = node.Successor
		fmt.Println("successor of hash is: ", retval)
		res := Response{retval, nil}
		encoder.Encode(res)
	} else {
		encoder2, decoder2 := createConnection(node.Successor)
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
func (node *ChordNode)fix_fingers() {
	finger_value := ""
	fmt.Println("fix fingers")
	index := 0
	for i := uint64(0); i < node.M; i++ {
		fmt.Println("iteration: ", i+1)
		start := uint64((node.HashID + powerof(2,i)) % powerof(2,node.M))
		fmt.Println("finger should be: ", start)
		if(inChordRange(start, generateNodeHash(node.Me, node.M), generateNodeHash(node.Successor, node.M), node.M)) {
			fmt.Println("in chord range")
			finger_value = node.Successor
		} else if(inChordRange(start, generateNodeHash(node.Predecessor, node.M), generateNodeHash(node.Me, node.M), node.M)) {
			fmt.Println("i'm my finger")
			finger_value = node.Me
		} else if (node.Me != node.Successor) {
			encoder, decoder := createConnection(node.Successor)
			fmt.Println("making request... with ", start)
			m := Request{"successor_of_hash", start}
			encoder.Encode(m)
			res := new(Response)
			decoder.Decode(&res)
			finger_value = res.Result.(string)
		} else {
			fmt.Println("we're our finger")
			finger_value = node.Me
		}
		node.Fingers[index] = finger_value
		index++
	}
}

//not relevant; won't have node failures. can be set on node shutdown
//func check_predecessor()

func query_key(key string, triplets *db.Col)  []map[string]interface{}{

	var query interface{}

	json.Unmarshal([]byte(`{"eq": "` + key + `", "in": ["key"]}`), &query)

	q_result := make(map[int]struct{}) // query result (document IDs) goes into map keys

	if err := db.EvalQuery(query, triplets, &q_result); err != nil {
		panic(err)
	}

	resultList := make([]map[string]interface{}, 0)
	if len(q_result) != 0 {
		for i := range q_result {
			readBack, err := triplets.Read(i)
			if err != nil {
				panic(err)
			}
			
			val := readBack["val"].(map[string]interface{})
			rel := readBack["rel"].(string)
			//update with new Accessed time
			val["Accessed"] = time.Now().Unix()
			updateTripletValue(i, key, rel, val, triplets)
			
			resultList = append(resultList, val)
		}
	}
	return resultList
}

func query_rel(rel string, triplets *db.Col) []map[string]interface{}{

	var query interface{}

	json.Unmarshal([]byte(`{"eq": "` + rel + `", "in": ["rel"]}`), &query)

	q_result := make(map[int]struct{}) // query result (document IDs) goes into map keys

	if err := db.EvalQuery(query, triplets, &q_result); err != nil {
		panic(err)
	}

	resultList := make([]map[string]interface{}, 0)
	if len(q_result) != 0 {
		for i := range q_result {
			readBack, err := triplets.Read(i)
			if err != nil {
				panic(err)
			}
			
			val := readBack["val"].(map[string]interface{})
			key := readBack["key"].(string)
			//update with new Accessed time
			val["Accessed"] = time.Now().Unix()
			updateTripletValue(i, key, rel, val, triplets)
			
			resultList = append(resultList, val)
		}
	}
	return resultList
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

func (n *ChordNode)lookup(req *Request, encoder *json.Encoder) {
	triplets := n.Dict3

	p := req.Params
	arr := p.([]interface{})
	
	key := arr[0].(string)
	rel := arr[1].(string)
	
	if (rel == "") {
		n.lookup_keyonly(req, encoder)
	} else if (key == "") {
		n.lookup_relonly(req, encoder)
	} else {
		// See if there this key/val is already in DB
		queryResult := query_key_rel(key, rel, triplets)
		if len(queryResult) != 0 {
			for i := range queryResult {
				readBack, err := triplets.Read(i)
				if err != nil {
					panic(err)
				}
				
				val := readBack["val"].(map[string]interface{})
				//update with new Accessed time
				val["Accessed"] = time.Now().Unix()
				updateTripletValue(i, key, rel, val, triplets)
				//send response
				m := Response{val, nil}
				encoder.Encode(m)
			}
		} else {	// Key/rel not in DB
			//get hash for key/rel
			keyRelHash := generateKeyRelHash(key, rel, n.M)
			//find closest successor/finger node for keyRelHash
			successor := n.get_successor_of_hash(keyRelHash)
			if (successor != n.Me) {
				//forward request to successor
				forwardEncoder, decoder := createConnection(successor)
				forwardEncoder.Encode(req)
				resp := new(Response)
				decoder.Decode(&resp)
				encoder.Encode(resp)
			} else {
				resp := Response{nil, nil}
				encoder.Encode(resp)
			}
		}
	}
}

func (n *ChordNode)lookup_keyonly(req *Request, encoder *json.Encoder) {
	triplets := n.Dict3
	
	p := req.Params
	arr := p.([]interface{})
	key := arr[0].(string)
	
	//begin building result list of satisfied keys
	resultList := query_key(key, triplets)
	
	keyPartialHashes := generateKeyPartialHashes(key, n.M)
	for (n.get_successor_of_hash(keyPartialHashes[0]) == n.Me) {
		keyPartialHashes = keyPartialHashes[1:]
		if (len(keyPartialHashes) == 0) {
			break
		}
	}
	if (len(keyPartialHashes) > 0) {		// only forward if hashes remain
		successor := n.get_successor_of_hash(keyPartialHashes[0])
		forwardEncoder, decoder := createConnection(successor)
		forwardedParams := make([]interface{}, 2)
		forwardedParams[0] = key
		forwardedParams[1] = keyPartialHashes
		req := Request{Method: "lookup_keyonly_internal", Params: forwardedParams}
		forwardEncoder.Encode(req)
		forwardedResponse := new(Response)
		decoder.Decode(&forwardedResponse)
		
		//add response to result list
		forwardedResults := forwardedResponse.Result
		fArr := forwardedResults.([]interface{})
		for _, result := range fArr {
			resultList = append(resultList, result.(map[string]interface{}))
		}
	}
	
	resp := Response{resultList, nil}
	encoder.Encode(resp)
}

// internal method for key-only lookups that passes a list of potential matched hashes
func (n *ChordNode)lookup_keyonly_internal(req *Request, encoder *json.Encoder) {
	triplets := n.Dict3
	
	p := req.Params
	arr := p.([]interface{})
	key := arr[0].(string)
	keyPartialHashes_temp := arr[1].([]interface{})
	keyPartialHashes := make([]uint64, len(keyPartialHashes_temp))
	for i, elem := range keyPartialHashes_temp {
		keyPartialHashes[i] = uint64(elem.(float64))	//don't ask, man. don't ask.
	}
	
	resultList := query_key(key, triplets)
	
	for (n.get_successor_of_hash(keyPartialHashes[0]) == n.Me) {
		keyPartialHashes = keyPartialHashes[1:]
		if (len(keyPartialHashes) == 0) {
			break
		}
	}

	if (len(keyPartialHashes) > 0) {		// only forward if hashes remain
		successor := n.get_successor_of_hash(keyPartialHashes[0])
		forwardEncoder, decoder := createConnection(successor)
		forwardedParams := make([]interface{}, 2)
		forwardedParams[0] = key
		forwardedParams[1] = keyPartialHashes
		req := Request{Method: "lookup_keyonly_internal", Params: forwardedParams}
		forwardEncoder.Encode(req)
		forwardedResponse := new(Response)
		decoder.Decode(&forwardedResponse)
		
		//add response to result list
		forwardedResults := forwardedResponse.Result
		fArr := forwardedResults.([]interface{})
		for _, result := range fArr {
			resultList = append(resultList, result.(map[string]interface{}))
		}
	}

	resp := Response{resultList, nil}
	encoder.Encode(resp)
}

func (n *ChordNode)lookup_relonly(req *Request, encoder *json.Encoder) {
	triplets := n.Dict3
	
	p := req.Params
	arr := p.([]interface{})
	rel := arr[1].(string)
	
	//begin building result list of satisfied keys
	resultList := query_rel(rel, triplets)
	
	relPartialHashes := generateRelPartialHashes(rel, n.M)
	for (n.get_successor_of_hash(relPartialHashes[0]) == n.Me) {
		relPartialHashes = relPartialHashes[1:]
		if (len(relPartialHashes) == 0) {
			break
		}
	}
	if (len(relPartialHashes) > 0) {		// only forward if hashes remain
		successor := n.get_successor_of_hash(relPartialHashes[0])
		forwardEncoder, decoder := createConnection(successor)
		forwardedParams := make([]interface{}, 2)
		forwardedParams[0] = rel
		forwardedParams[1] = relPartialHashes
		req := Request{Method: "lookup_relonly_internal", Params: forwardedParams}
		forwardEncoder.Encode(req)
		forwardedResponse := new(Response)
		decoder.Decode(&forwardedResponse)
		
		//add response to result list
		forwardedResults := forwardedResponse.Result
		fArr := forwardedResults.([]interface{})
		for _, result := range fArr {
			resultList = append(resultList, result.(map[string]interface{}))
		}
	}
	
	resp := Response{resultList, nil}
	encoder.Encode(resp)
}

// internal method for rel-only lookups that passes a list of potential matched hashes
func (n *ChordNode)lookup_relonly_internal(req *Request, encoder *json.Encoder) {
	triplets := n.Dict3
	
	p := req.Params
	arr := p.([]interface{})
	rel := arr[0].(string)
	relPartialHashes_temp := arr[1].([]interface{})
	relPartialHashes := make([]uint64, len(relPartialHashes_temp))
	for i, elem := range relPartialHashes_temp {
		relPartialHashes[i] = uint64(elem.(float64))	//don't ask, man. don't ask.
	}
	
	resultList := query_rel(rel, triplets)
	
	for (n.get_successor_of_hash(relPartialHashes[0]) == n.Me) {
		relPartialHashes = relPartialHashes[1:]
		if (len(relPartialHashes) == 0) {
			break
		}
	}

	if (len(relPartialHashes) > 0) {		// only forward if hashes remain
		successor := n.get_successor_of_hash(relPartialHashes[0])
		forwardEncoder, decoder := createConnection(successor)
		forwardedParams := make([]interface{}, 2)
		forwardedParams[0] = rel
		forwardedParams[1] = relPartialHashes
		req := Request{Method: "lookup_keyonly_internal", Params: forwardedParams}
		forwardEncoder.Encode(req)
		forwardedResponse := new(Response)
		decoder.Decode(&forwardedResponse)
		
		//add response to result list
		forwardedResults := forwardedResponse.Result
		fArr := forwardedResults.([]interface{})
		for _, result := range fArr {
			resultList = append(resultList, result.(map[string]interface{}))
		}
	}

	resp := Response{resultList, nil}
	encoder.Encode(resp)
}

//needs a time to purge 'older than' entries
//key:nil rel:nil value: date to purge after
//sample date "2015-05-08T16:55:55.326107309-04:00"

func (n *ChordNode)purge(req *Request, encoder *json.Encoder){

	fmt.Println("entering purge")
	triplets := n.Dict3
	
	p := req.Params
	arr := p.([]interface{})

	val := arr[2].(string)
	
	time_int, err := strconv.ParseInt(val, 10, 64) 
	if err != nil {
		panic(err)
	}
	
	purge_time := time.Unix(time_int, 0)
	purge_list := make([]int, 0)
	
	triplets.ForEachDoc(func(id int, docContent []byte) (willMoveOn bool) {
		fmt.Println("Document", id, "is", string(docContent))

		readBack, err := triplets.Read(id)
		if err != nil {
			panic(err)
		}
			
		dictVal := readBack["val"].(map[string]interface{})
		//Check permissions before deleting, can't delete if "R"
		accessed_time := int64(dictVal["Accessed"].(float64))
		
		if time.Unix(accessed_time, 0).Before(purge_time) {
			purge_list = append(purge_list, id)
		}
		
		return true  // move on to the next document OR
	})

	fmt.Println("triplets to purge")
	for i := 0; i < len(purge_list); i++ {
		readBack, err := triplets.Read(purge_list[i])
		if err != nil {
			panic(err)
		}
			
	 	dictVal := readBack["val"].(map[string]interface{})
		//Check permissions before deleting, can't delete if "R"
		if dictVal["Permission"] == "RW" {
			fmt.Println("deleting ", purge_list[i])
			if err := triplets.Delete(purge_list[i]); err != nil {
				panic(err)
			}
		}
	}
}

func (n *ChordNode)delete(req *Request, encoder *json.Encoder){
	triplets := n.Dict3
	
	p := req.Params
	arr := p.([]interface{})
	
	key := arr[0].(string)
	rel := arr[1].(string)

	queryResult := query_key_rel(key, rel, triplets)

	if (len(queryResult) > 0) {
		for i := range queryResult {
			readBack, err := triplets.Read(i)
			if err != nil {
				panic(err)
			}
				
			dictVal := readBack["val"].(map[string]interface{})
			//Check permissions before deleting, can't delete if "R"
			if dictVal["Permission"] == "RW" {
				fmt.Println("deleting ", i)
				if err := triplets.Delete(i); err != nil {
					panic(err)
				}
			}
		}
	} else {
		keyRelHash := generateKeyRelHash(key, rel, n.M)
		//find closest successor/finger node for keyRelHash
		successor := n.get_successor_of_hash(keyRelHash)
		//forward request to successor
		forwardEncoder, decoder := createConnection(successor)
		forwardEncoder.Encode(req)
		resp := new(Response)
		decoder.Decode(&resp)
		encoder.Encode(resp)
	}	
}

func (n *ChordNode)insert(req *Request, encoder *json.Encoder, update bool){
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
	successor := n.get_successor_of_hash(keyRelHash)
	fmt.Println("successor: " + successor)
	if (successor != n.Me) {
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
					if dictVal["Permission"] == "R" {
						//No return value for insertOrUpdate() so returning silently
						return
					}

					//update content
					dictVal["Content"] = val
					//update with new Accessed/Modified time
					now := time.Now().Unix()
					dictVal["Accessed"] = now
					dictVal["Modified"] = now
					updateTripletValue(i, key, rel, dictVal, triplets)
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

	switch(req.Method) {
	case "find_successor" :
		node.find_successor(req, encoder)
	//case "find_predecessor" :
	//	find_predecessor(req, encoder)
	case "get_successor" :
		node.get_successor(req, encoder)
	case "get_predecessor" :
		node.get_predecessor(req, encoder)
	case "set_successor" :
		node.set_successor(req, encoder)
	case "set_predecessor" :
		node.set_predecessor(req, encoder)
	//case "update_finger_table" :
		//node.update_finger_table(node, req, encoder)
	case "notify" :
		node.notify(req, encoder)
	case "lookup" :
		node.lookup(req, encoder)
	case "lookup_keyonly_internal" :
		node.lookup_keyonly_internal(req, encoder)
	case "lookup_relonly_internal" :
		node.lookup_relonly_internal(req, encoder)
	case "insert" :
		node.insert(req, encoder, false)
	case "insertOrUpdate" :
		node.insert(req, encoder, true)
	case "delete" :
		node.delete(req, encoder)
	case "purge" :
		node.purge(req, encoder)
	}
}

func main() {
	// Parse argument configuration block
	config := *readConfig()
	node := create(config)

	if len(os.Args) > 2 {
		ringNodeAddr := os.Args[2]
		node.join(ringNodeAddr)
	} else {
		fmt.Println("Creating a new chord ring...")
		node.create()
	}

	fmt.Println("node: "+node.Me)
	fmt.Println("node successor: "+node.Successor)
	fmt.Println("node hashID: ", node.HashID)

	fmt.Println("Setting up a listener...")
	
	l, err := net.Listen(config.Protocol, node.Me)
	if err != nil {
		log.Fatal("Listen error:", err)
	}

	fmt.Println("Listening on ", node.Me)	

	//ticker for stabilize
	ticker := time.NewTicker(time.Millisecond * 5000)
	go func() {
		for t := range ticker.C {
			node.stabilize()
			//fmt.Println("node identifier: "+node.Me)
			//fmt.Println("node successor: "+node.Successor)
			//fmt.Println("node predecessor: "+node.Predecessor)
			//fmt.Println("node hashID: ", node.HashID)

			_ = t
		}
	}()
/*
	ticker2 := time.NewTicker(time.Millisecond * 20000)
	go func() {
		for t2 := range ticker2.C {
			node.fix_fingers()
			fmt.Println("fingers[0]: ", node.Fingers[0])
			fmt.Println("fingers[1]: ", node.Fingers[1])
			fmt.Println("fingers[2]: ", node.Fingers[2])

			_ = t2
		}
	}()
*/
    //Listen for a connection
    for {
    	conn, err := l.Accept() // this blocks until connection or error
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		}
		go handleConnection(node, conn)
    }
}
