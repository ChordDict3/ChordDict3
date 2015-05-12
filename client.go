package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"bufio"
	"time"
	"io/ioutil"
)

type Configuration struct{
	ServerID string `json:"serverID"`
	Protocol string `json:"protocol"`
	IpAddress string `json:"ipAddress"`
	Port string `json:"port"`
	M uint64 `json:"M"`
	//PersistentStorageContainer struct {
		//File string `json:"file"`
	//} `json:"persistentStorageContainer"`
	Methods []string `json:"methods"`
}
func readInput(config *Configuration){

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan(){
		text := scanner.Text()

		networkaddress := config.IpAddress + ":" + config.Port
		conn, err := net.Dial(config.Protocol, networkaddress)
		if err != nil {
			log.Fatal("Connection error", err)
		}

		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
			
		ip_writer := bufio.NewWriter(conn)
		ip_reader := bufio.NewReader(conn)
		
		fmt.Println("Sending: ", text)
			
		ip_writer.WriteString(text)
		ip_writer.Flush()
		
		line, _ := ip_reader.ReadString('\n')
		fmt.Println("Recieved: ", line)

		conn.Close()
	}

		
}

func readConfig()(config *Configuration){

	dat, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		fmt.Println("Error reading file")
		panic(err)
	}


	b_arr := []byte(string(dat))

	config = new(Configuration)
	if err := json.Unmarshal(b_arr, &config); err != nil {
		panic(err)
	}

	return config

}


func main() {
	
	config := readConfig()
	readInput(config)
	
}
