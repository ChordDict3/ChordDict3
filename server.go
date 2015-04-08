package main

import (
	//"fmt"
	"net"
	"encoding/json"
	"io/ioutil"
	"os"
	"github.com/HouzuoGuo/tiedot/db"
	"strconv"
)

/*
TODO
ID
finish parsing values from config
*/

type Request struct{
	Method string `json:"method"` 
	Params interface{} `json:"params`
	Id interface{} `json:"id"`
}

type Response struct{
	Result interface{} `json:"result"`
	Id interface{} `json:"id"`
	Error interface{} `json:"error"`      // Error must be null if there was no error
}

type Easy struct{
	Easy string `json:"easy"`
}

type Configuration struct{
	ServerID string `json:"serverID"`
	Protocol string `json:"protocol"`
	IpAddress string `json:"ipAddress"`
	Port int `json:"port"`
	PersistentStorageContainer struct {
		File string `json:"file"`
	} `json:"persistentStorageContainer"`
	Methods []string `json:"methods"`

}

func handleConnection(conn net.Conn, triplets *db.Col, myDB *db.DB) {
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	req := new(Request)
	decoder.Decode(&req)
	//fmt.Println()
	//fmt.Printf("Received : %+v", req)
	//fmt.Println()
	
	// Switch to see what method to call
	
	switch req.Method {
	case "lookup" :
		lookup(req, encoder, triplets, req.Id)
	case "insert" :
		insert(req, encoder, triplets, false, req.Id)
	case "insertOrUpdate":
		insert(req, encoder, triplets, true, req.Id)
	case "delete" :
		delete(req, triplets)
	case "listKeys" :
		listKeys(encoder, triplets, req.Id)
	case "listIDs" :
		listIDs(encoder, triplets, req.Id)
	case "shutdown" :
		shutdown(myDB)
	}
	
}

func lookup(req *Request, encoder *json.Encoder, triplets *db.Col, id interface{}){
	//fmt.Printf("Looking up %v", req)

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
			
			val := readBack["val"].(map[string]interface {})
			//fmt.Println(val)
			//fmt.Println(key + " " + rel + " has value " + val)
			//TODO get ID value
			m := Response{val, id, nil}
			encoder.Encode(m)
		}
		
	} else {
		// Key/rel not in DB
		//fmt.Println("key/rel not in DB return null in result")
		m := Response{nil, id, nil}
		encoder.Encode(m)
	}
	
}

func listKeys(encoder *json.Encoder, triplets *db.Col, id interface{}){
	//TODO make sure UNIQUE keys
	//fmt.Println("Listing all unique keys")

	var query interface{}
	json.Unmarshal([]byte(`{"n": [{"has": ["key"]}, {"has": ["rel"]}]}`), &query)
	//json.Unmarshal([]byte(`{"eq": "keyA", "in": ["key"]}`), &query)
	queryResult := make(map[int]struct{}) // query result (document IDs) goes into map keys

	if err := db.EvalQuery(query, triplets, &queryResult); err != nil {
		panic(err)
	}

	key_set := make(map[string]bool)
	// Query result are document IDs
	//fmt.Println(queryResult)
	for id := range queryResult {

		readBack, err := triplets.Read(id)
		if err != nil {
			panic(err)
		}
		
		key_set[readBack["key"].(string)] = true
	}

	val := make([]string, 0)
	for i := range key_set{
		//fmt.Println(i)
		val = append(val,i)
	}
	
	m := Response{val, id, nil}
	encoder.Encode(m)
}

func listIDs(encoder *json.Encoder, triplets *db.Col, id interface{}){
	//TODO make sure UNIQUE keys
	//fmt.Println("Listing all unique IDs")

	var query interface{}
	json.Unmarshal([]byte(`{"n": [{"has": ["key"]}, {"has": ["rel"]}]}`), &query)
	//json.Unmarshal([]byte(`{"eq": "keyA", "in": ["key"]}`), &query)
	queryResult := make(map[int]struct{}) // query result (document IDs) goes into map keys

	if err := db.EvalQuery(query, triplets, &queryResult); err != nil {
		panic(err)
	}

	//fmt.Println(queryResult)

	id_set := make(map[[2]string]bool)
	// Query result are document IDs
	for id := range queryResult {

		readBack, err := triplets.Read(id)
		if err != nil {
			panic(err)
		}
		//fmt.Println(readBack)
		id_set[[2]string{readBack["key"].(string), readBack["rel"].(string)}] = true 
	}

	val := make([]interface{}, 0)
	for i := range id_set{
		//fmt.Println(i)
		val = append(val, i)
	}
		
	m := Response{val, id, nil}
	encoder.Encode(m)
	
}


func insert(req *Request, encoder *json.Encoder, triplets *db.Col, update bool, id interface{}){
	//fmt.Printf("Inserting %v", req)

	p := req.Params
	arr := p.([]interface{})
	
	key := arr[0].(string)
	rel := arr[1].(string)
	val := arr[2]

	//fmt.Println(key, rel, val)

	// See if there this key/val is already in DB
	queryResult := query_key_rel(key, rel, triplets)
	if len(queryResult) != 0 {
		//Key Already Exists
		//fmt.Println("Insert: key " + key + " rel " + rel + " already exists")

		if update{
			// insertOrUpdate() now replaces the key/rel with an updated value
			// delete old value, insert new
			for id := range queryResult {
				//fmt.Println("Deleting ", id)
				if err := triplets.Delete(id); err != nil {
					panic(err)
				}
			}

			//insert new value
			_, err := triplets.Insert(map[string]interface{}{
				"key": key,
				"rel": rel,
				"val": val})
			if err != nil {
				panic(err)
			}
			//fmt.Println("Inserting ", docID)

			//insertOrUpdate doesn't return anything


		} else{
			// insert() fails if key/rel already exists
			//fmt.Println("Insert did not happen, need to return false")

			// TODO difference in spec, json RPC says to set "result" to null if error, project spec says to return "false"
			m := Response{false, id, nil}
			encoder.Encode(m)

			
		}
	} else {
		
		_, err := triplets.Insert(map[string]interface{}{
			"key": key,
			"rel": rel,
			"val": val})
		if err != nil {
			panic(err)
		}
		//fmt.Println("Inserting ", docID)
		
		//insertOrUpdate doesn't have a return value
		if update == false {
			m := Response{true, id, nil}
			encoder.Encode(m)
		}

	}
}


func delete(req *Request, triplets *db.Col){
	
	p := req.Params
	arr := p.([]interface{})
	
	key := arr[0].(string)
	rel := arr[1].(string)


	queryResult := query_key_rel(key, rel, triplets)

	// Query result are document IDs
	for id := range queryResult {
		//fmt.Println("Deleting ", id)
		if err := triplets.Delete(id); err != nil {
			panic(err)
		}
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

func shutdown(myDB *db.DB){
	//fmt.Println("Shutting Down DB")
	myDB.Close()
	os.Exit(0)
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
	config := readConfig()
	
	//DB code
	
	//fmt.Println("Intalizing DB")
	myDBDir := config.PersistentStorageContainer.File

	// (Create if not exist) open a database
	myDB, err := db.OpenDB(myDBDir)
	if err != nil {
		panic(err)
	}

	defer myDB.Close()
	
	if err := myDB.Create("Triplets"); err != nil {
		//panic(err)
		//fmt.Println(err.Error())
	}

	
	triplets := myDB.Use("Triplets")


	// Create indexes here??
	// TODO: Do not create index if it already exists?
	if err := triplets.Index([]string{"key"}); err != nil {
		//panic(err)
		//fmt.Println(err.Error())
	}
        
	if err := triplets.Index([]string{"rel"}); err != nil {
		//panic(err)
		//fmt.Println(err.Error())
	}

	networkaddress := config.IpAddress + ":" + strconv.Itoa(config.Port)
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
		go handleConnection(conn, triplets, myDB) // a goroutine handles conn so that the loop can accept other connections
	}
}
