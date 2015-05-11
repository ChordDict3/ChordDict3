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
    "strconv"
	"github.com/HouzuoGuo/tiedot/db"
)

type ChordNode struct{
	Me, Successor, Predecessor string
	FingerTable []string
	HashID uint64
	M uint64 
	Dict3 *db.Col
	Keys []uint64
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
	Port int `json:"port"`
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

//////////////////
//HELPER FUNCTIONS
//////////////////
//create connection to other nodes
func createConnection(netAddr string) (encoder *json.Encoder, decoder *json.Decoder){
	fmt.Println("Creating connection to ", netAddr)
	conn, err := net.Dial("tcp", netAddr)

	if err != nil {
		fmt.Println("Connection error", err)
        return nil, nil
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

func create(config Configuration) *ChordNode{
    port := strconv.FormatInt(int64(config.Port), 10)
	netAddr := config.IpAddress + ":" + port
	hash := generateNodeHash(netAddr, config.M)
	
	myDBDir := config.PersistentStorageContainer.File
	// (Create if not exist) open a database
	myDB, err := db.OpenDB(myDBDir)
	if err != nil {
		panic(err)
	}
	dbName := "Triplets"+port
	if err := myDB.Create(dbName); err != nil {
		panic(err)
	}	
	triplets := myDB.Use(dbName)
	// Create indexes here??
	// TODO: Do not create index if it already exists? --- the Index function already handles this
    if err := triplets.Index([]string{"hash"}); err != nil {
        panic(err)
    }
    if err := triplets.Index([]string{"key"}); err != nil {
        panic(err)
    }
    if err := triplets.Index([]string{"rel"}); err != nil {
        panic(err)
    }
	c := ChordNode{
		Me: netAddr,
		Successor: "",
		Predecessor: "",
		FingerTable: make([]string, config.M),
		HashID: hash,
		M: config.M,
		Dict3: triplets,
        Keys: make([]uint64, 0),
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

/*
func status(node *ChordNode, msg string){
	fmt.Println("Ident: ", node.Me, " P: ", node.Predecessor, " S: ", node.Successor, " F: ", node.Finger, " ",  msg)

} 
*/

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

func (node *ChordNode)transfer_keys_on_shutdown(req *Request, encoder *json.Encoder) {    
    fmt.Println("entered transfer_keys_on_shutdown")
    forwarded_triplets := req.Params.([]interface{})
    
    triplets := node.Dict3
    
    for _, t := range forwarded_triplets {
        triplet := t.(map[string]interface{})
        keyRelHash := uint64(triplet["hash"].(float64))
        _, err := triplets.Insert(map[string]interface{}{
            "hash": keyRelHash,
            "key": triplet["key"].(string),
            "rel": triplet["rel"].(string),
            "val": triplet["val"].(map[string]interface{})})
        if err != nil {
            panic(err)
        }
        node.Keys = append(node.Keys, keyRelHash)
    }
    
    m := Response{"Success", nil}
    encoder.Encode(m)
    return
}

func (node *ChordNode)transfer_keys_on_join(req *Request, encoder *json.Encoder) {
    fmt.Println("entered transfer_keys_on_join")
    forwarded_hashID := uint64(req.Params.(float64))
    
    return_triplets := make([]interface{}, 0)
    triplets := node.Dict3
    
    // Loop through successor keys and check if key# <= node#
    for _, value := range node.Keys {
        if (value <= forwarded_hashID) {
            queryResult := query_hash(value, triplets)
            if len(queryResult) != 0 {
                for i := range queryResult {
                    readBack, err := triplets.Read(i)
                    if err != nil {
                        panic(err)
                    }
                    if err := triplets.Delete(i); err != nil {
                        panic(err)
                    }
                    return_triplets = append(return_triplets, readBack)
                }
                
                // Delete hash from successor's Keys
                keyRelHashIndex := node.find_key(value)
                if (keyRelHashIndex > -1) {
                    node.Keys = append(node.Keys[:keyRelHashIndex], node.Keys[keyRelHashIndex+1:]...)
                }
            }
        }
    }
    
    // Return the Dict3 values and Keys to the node joining the ring
    res := Response{return_triplets, nil}
    encoder.Encode(res)
    return
}

func (node *ChordNode)successor_of_hash(hash uint64) string {
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
    if (encoder == nil && decoder == nil) { // Something went wrong
        fmt.Println("Join failed because Chord node with network address " + netAddr + " does not exist.")
        os.Exit(1)
        return
    }
    
    m := Request{"find_successor", node.Me}
    encoder.Encode(m)
    res := new(Response)
    decoder.Decode(&res)
    node.Successor = res.Result.(string)
    
	// Ask successor for all DICT3 data we should take from him
    successor_encoder, successor_decoder := createConnection(node.Successor)
    if (successor_encoder == nil && successor_decoder == nil) { // Something went wrong
        fmt.Println("Could not connect to current node's successor to take necessary keys and Dict3 data.")
        os.Exit(1)
        return
    }
    
    m = Request{"transfer_keys_on_join", node.HashID}
    successor_encoder.Encode(m)
    res = new(Response)
    successor_decoder.Decode(&res)

    returned_triplets := res.Result.([]interface{})
    triplets := node.Dict3
    
    for _, t := range returned_triplets {
        triplet := t.(map[string]interface{})
        keyRelHash := uint64(triplet["hash"].(float64))
        _, err := triplets.Insert(map[string]interface{}{
            "hash": keyRelHash,
            "key": triplet["key"].(string),
            "rel": triplet["rel"].(string),
            "val": triplet["val"].(map[string]interface{})})
        if err != nil {
            panic(err)
        }
        node.Keys = append(node.Keys, keyRelHash)
    }
    
    return
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
		encoder2, decoder2 := createConnection(node.Successor) //change to closest finger
		//closest_preceding_node := node.closest_preceding_node(generateNodeHash(netAddr, node.M))
		//encoder2, decoder2 := createConnection(closest_preceding_node)
        if (encoder2 == nil && decoder2 == nil) { // Something went wrong
            fmt.Println("Could not connect to the current node's successor to find its successor.")
            os.Exit(1)
            return
        }
        fmt.Println("Looking for successor")
        m := Request{"find_successor", netAddr}
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
    if (encoder != nil && decoder != nil) { // If either of these are nil because of shutdown, we can just stabilize later
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
            if (encoder != nil && decoder != nil) { // We can stabilize this later if shutdown is currently happening
                m = Request{"notify", node.Me}
                encoder.Encode(m)
            }
        } else if (inChordRange(generateNodeHash(node.Me, node.M), generateNodeHash(successors_predecessor, node.M), generateNodeHash(node.Successor, node.M), node.M)) {
            // me < succ_pred < succ; successor's predecessor should be my successor
            if(node.Me != node.Successor) {	
                fmt.Println("going to notify ", node.Successor, "that ", node.Me, "should be his predecesor")	
                encoder, decoder = createConnection(node.Successor)
                if (encoder != nil && decoder != nil) { // We can stabilize this later if shutdown is currently happening
                    m = Request{"notify", node.Me}
                    encoder.Encode(m)
                }
            }
        }
    }
}

//check if netAddr should be this node's predecessor
func (node *ChordNode)notify(req *Request, encoder *json.Encoder) {
	netAddr := req.Params.(string)
	if (node.Predecessor == "") { 
	//node has no predecessor, set netAddr as node's predecessor
		node.Predecessor = netAddr
		res := Response{netAddr, nil}
		encoder.Encode(res)
	} else if (inChordRange(generateNodeHash(netAddr, node.M), generateNodeHash(node.Predecessor, node.M), generateNodeHash(node.Me, node.M), node.M)) { 
	//predecessor < netAddr < node; ID should be pred
		node.Predecessor = netAddr
		res := Response{netAddr, nil}
		encoder.Encode(res)
	} else { 
	//node's predecessor was already correct
		res := Response{node.Predecessor, nil}
		encoder.Encode(res)
	}
}

func (node *ChordNode)successor_of_hash_rpc(req *Request, encoder *json.Encoder) {
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
		//encoder2, decoder2 := createConnection(node.Successor) //should change this to closest finger
		closest_preceding_node := node.closest_preceding_node(hash)
		encoder2, decoder2 := createConnection(closest_preceding_node)
        if (encoder2 == nil && decoder2 == nil) { // Something went wrong
            fmt.Println("Could not connect to the current node's successor to get its successor of hash via RPC.")
            os.Exit(1)
            return
        }
        m := Request{"successor_of_hash_rpc", hash}
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
		start := uint64((node.HashID + powerof(2,i)) % powerof(2,node.M))
		if(inChordRange(start, generateNodeHash(node.Me, node.M), generateNodeHash(node.Successor, node.M), node.M)) {
			finger_value = node.Successor
		} else if(inChordRange(start, generateNodeHash(node.Predecessor, node.M), generateNodeHash(node.Me, node.M), node.M)) {
			finger_value = node.Me
		} else if (node.Me != node.Successor) {
			encoder, decoder := createConnection(node.Successor)
            if (encoder != nil && decoder != nil) { // We can fix_fingers later if shutdown is currently happening
                m := Request{"successor_of_hash_rpc", start}
                encoder.Encode(m)
                res := new(Response)
                decoder.Decode(&res)
                finger_value = res.Result.(string)
            } else {
                return
            }
		} else {
			finger_value = node.Me
		}
		node.FingerTable[index] = finger_value
		index++
	}
}

func (node *ChordNode)closest_preceding_node(hash uint64) string {
	for i := uint64(1); i < node.M; i++ {
		if(node.FingerTable[i-1] != "" && node.FingerTable[i] != "") {
			if(inChordRange(hash, generateNodeHash(node.FingerTable[i-1], node.M), generateNodeHash(node.FingerTable[i], node.M), node.M)) {
				return node.FingerTable[i-1]
			}
		}
	}
	//no finger was closest; return self
	return node.Me
}

//not relevant; won't have node failures. can be set on node shutdown
//func check_predecessor()

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
func insertValueIntoTriplets(keyRelHash uint64, key string, rel string, val map[string]interface{}, triplets *db.Col) {
	_, err := triplets.Insert(map[string]interface{}{
        "hash": keyRelHash,
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

func query_hash(hash uint64, triplets *db.Col) (queryResult map[int]struct{}) {

    var query interface{}

	json.Unmarshal([]byte(`{"eq": hash, "in": ["hash"]}`), &query)

	q_result := make(map[int]struct{}) // query result (document IDs) goes into map keys

	if err := db.EvalQuery(query, triplets, &q_result); err != nil {
		panic(err)
	}

	return q_result
}

func (node *ChordNode) find_key(key uint64) int {
    for index, value := range node.Keys {
        if (value == key) {
            return index
        }
    }
    return -1
}

func (n *ChordNode)lookup(req *Request, encoder *json.Encoder) {
	triplets := n.Dict3

	p := req.Params
	arr := p.([]interface{})
	
	key := arr[0].(string)
	rel := arr[1].(string)
    
    //get hash for key/rel
	keyRelHash := generateKeyRelHash(key, rel, n.M)

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
			insertValueIntoTriplets(keyRelHash, key, rel, val, triplets)
			//send response
			m := Response{val, nil}
			encoder.Encode(m)
		}
		
	} else {	// Key/rel not in DB
		//find closest successor/finger node for keyRelHash
		successor := n.successor_of_hash(keyRelHash)
		//forward request to successor
		forwardEncoder, decoder := createConnection(successor)
        if (forwardEncoder == nil && decoder == nil) { // Something went wrong
            fmt.Println("Could not connect to the current node's successor to lookup data.")
            os.Exit(1)
            return
        }
		forwardEncoder.Encode(req)
		resp := new(Response)
		decoder.Decode(&resp)
		encoder.Encode(resp)
	}
}

func (n *ChordNode)delete(req *Request, encoder *json.Encoder){
    fmt.Println("entering delete")
    triplets := n.Dict3

	p := req.Params
	arr := p.([]interface{})
	
	key := arr[0].(string)
	rel := arr[1].(string)

	queryResult := query_key_rel(key, rel, triplets)

	for i := range queryResult {
		readBack, err := triplets.Read(i)
		if err != nil {
			panic(err)
		}
			
		dictVal := readBack["val"].(map[string]interface{})
		//Check permissions before deleting, can't delete if "R"
		if dictVal["Permission"] == "RW" {
			if err := triplets.Delete(i); err != nil {
				panic(err)
			}
            
            // Delete hash from node's Keys
            keyRelHashIndex := n.find_key(readBack["hash"].(uint64))
            if (keyRelHashIndex > -1) {
                n.Keys = append(n.Keys[:keyRelHashIndex], n.Keys[keyRelHashIndex+1:]...)
            }
		}
		
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
	successor := n.successor_of_hash(keyRelHash)
	fmt.Println("successor: " + successor)
    
	if (successor != n.Me) {
        forwardEncoder, decoder := createConnection(successor)
        if (forwardEncoder == nil && decoder == nil) { // Something went wrong
            fmt.Println("Could not connect to the current node's successor to insert data.")
            os.Exit(1)
            return
        }
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
					now := time.Now()
					dictVal["Accessed"] = now
					dictVal["Modified"] = now
					insertValueIntoTriplets(keyRelHash, key, rel, dictVal, triplets)
				}
			} else {
				// insert() fails if key/rel already exists
				m := Response{false, nil}
				encoder.Encode(m)
			}
		} else {
			dictVal := makeDictValue(val, "RW")
			_, err := triplets.Insert(map[string]interface{}{
                "hash": keyRelHash,
				"key": key,
				"rel": rel,
				"val": dictVal})
            n.Keys = append(n.Keys, keyRelHash)
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

func (n *ChordNode)shutdown(req *Request, encoder *json.Encoder) {
    // Get the node's hash ID
    //hashID := n.HashID
    // Set the hash ID to something unreachable
    n.HashID = powerof(2, n.M) + 1
    
    // Get the node's predecessor
    predecessor := n.Predecessor
    // Set the node's predecessor to nothing
    n.Predecessor = ""
    
    // Get the node's successor
    successor := n.Successor
    // Set the node's successor to nothing
    n.Successor = ""
    
    // If this is the only node in the ring
    if (successor == predecessor && successor == n.Me) {
        // TODO: dump database
    } else {
        // Copy all keys and Dict3 triplets from node to successor        
        successor_triplets := make([]interface{}, 0)
        triplets := n.Dict3
        for _, value := range n.Keys {
            queryResult := query_hash(value, triplets)
            if len(queryResult) != 0 {
                for i := range queryResult {
                    readBack, err := triplets.Read(i)
                    if err != nil {
                        panic(err)
                    }
                    if err := triplets.Delete(i); err != nil {
                        panic(err)
                    }
                    successor_triplets = append(successor_triplets, readBack)
                }
            }
        }
        successor_encoder, successor_decoder := createConnection(successor)
        if (successor_encoder == nil && successor_decoder == nil) { // Something went wrong
            fmt.Println("Could not connect to the current node's successor to transfer keys and Dict3 data.")
            os.Exit(1)
            return
        }
        
        m := Request{"transfer_keys_on_shutdown", successor_triplets}
        successor_encoder.Encode(m)
        res := new(Response)
        successor_decoder.Decode(&res)
        
        // Set predecessor's successor to current node's successor
        predecessor_encoder, predecessor_decoder := createConnection(predecessor)
        if (predecessor_encoder == nil && predecessor_decoder == nil) { // Something went wrong
            fmt.Println("Could not connect to the current node's predecessor to reset its successor.")
            os.Exit(1)
            return
        }
        m = Request{"set_successor", successor}
        predecessor_encoder.Encode(m)
        res = new(Response)
        predecessor_decoder.Decode(&res)
        
        // Set successor's predecessor to current node's predecessor
        successor_encoder, successor_decoder = createConnection(successor)
        if (successor_encoder == nil && successor_decoder == nil) { // Something went wrong
            fmt.Println("Could not connect to the current node's successor to reset its predecessor.")
            os.Exit(1)
            return
        }
        m = Request{"notify", predecessor}
        successor_encoder.Encode(m)
        res = new(Response)
        successor_decoder.Decode(&res)
        
        // Tell the successor to fix its fingers
        successor_encoder, successor_decoder = createConnection(successor)
        if (successor_encoder == nil && successor_decoder == nil) { // Something went wrong
            fmt.Println("Could not connect to the current node's successor to fix finger table.")
            os.Exit(1)
            return
        }
        m = Request{"fix_fingers", nil}
        successor_encoder.Encode(m)
        res = new(Response)
        successor_decoder.Decode(&res)
    }
    
	n.Keys = make([]uint64, 0)
    os.Exit(0)
    return
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
    case "transfer_keys_on_shutdown" :
        node.transfer_keys_on_shutdown(req, encoder)
    case "transfer_keys_on_join" :
        node.transfer_keys_on_join(req, encoder)
	case "find_successor" :
		node.find_successor(req, encoder)
	case "get_successor" :
		node.get_successor(req, encoder)
	case "get_predecessor" :
		node.get_predecessor(req, encoder)
	case "set_successor" :
		node.set_successor(req, encoder)
	case "set_predecessor" :
		node.set_predecessor(req, encoder)
	case "notify" :
		node.notify(req, encoder)
    case "successor_of_hash_rpc" :
        node.successor_of_hash_rpc(req, encoder)
    case "fix_fingers" :
        node.fix_fingers()
        encoder.Encode(Response{nil, nil})
	case "lookup" :
		node.lookup(req, encoder)
	case "insert" :
		node.insert(req, encoder, true)
	case "delete" :
		node.delete(req, encoder)
    case "shutdown" :
        node.shutdown(req, encoder)
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
			fmt.Println("node identifier: "+node.Me)
			fmt.Println("node successor: "+node.Successor)
			fmt.Println("node predecessor: "+node.Predecessor)
			fmt.Println("node hashID: ", node.HashID)

			_ = t
		}
	}()

		ticker2 := time.NewTicker(time.Millisecond * 20000)
	go func() {
		for t2 := range ticker2.C {
			node.fix_fingers()
			fmt.Println("fingertable[0]: ", node.FingerTable[0])
			fmt.Println("fingertable[1]: ", node.FingerTable[1])
			fmt.Println("fingertable[2]: ", node.FingerTable[2])

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
