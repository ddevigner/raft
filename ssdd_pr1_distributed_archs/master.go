/* ****************************************************************************************
 * AUTORES: Devid Dokash (780131)
 * ASIGNATURA: Sistemas Distribuidos
 * FECHA: Octubre de 2021
 * FICHERO: master.go
 * DESCRIPCIÓN: Codigo fuente del Master de la arquitectura Cliente-Master-Worker
 * ****************************************************************************************/
package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"

	"practica-1/com"

	"golang.org/x/crypto/ssh"
)

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

// Provides session sign connection associated with our private key
func session_key(file_name string) (sign ssh.Signer, err error) {
	// Getting private_key from file file_name
	priv_key, err := ioutil.ReadFile(file_name)
	if err != nil {
		return
	}

	// Creation of the session sign for establish connection.
	sign, err = ssh.ParsePrivateKey(priv_key)
	if err != nil {
		return
	}
	return
}

// Connects via ssh and runs the specified command.
func remote_ssh(id int, args []string, input bool) {
	// Processing it connection arguments.
	/*		args[0]= user
	 *		args[1]= ip
	 *		args[2]= port
	 *		args[3]= priv_key_path_file
	 *		args[4]= cmd cmd_arguments
	 */
	// Getting of the connection sign of our private key.
	sign, err := session_key(args[3])
	checkError(err)
	// Declaration of the ssh session connection configuration.
	config := &ssh.ClientConfig{
		User:            args[0],
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(sign)},
	}

	// Connection via TCP throught ip:args[1] and port:args[2] with config configuration.
	ssh_client, err := ssh.Dial("tcp", net.JoinHostPort(args[1], args[2]), config)
	checkError(err)
	defer ssh_client.Close()

	// Openning a new ssh session for the ssh client.
	ssh_session, err := ssh_client.NewSession()
	checkError(err)
	defer ssh_session.Close()

	// Redirection of standard output, input and error output.
	ssh_session.Stdout = os.Stdout
	if input {
		ssh_session.Stdin = os.Stdin
		return
	}
	ssh_session.Stderr = os.Stderr

	// Run the remote command
	err = ssh_session.Run(args[4])
	checkError(err)
}

// Worker handler, mantains worker connection and Request-Reply communication between Master and Worker.
func worker_handler(ln net.Listener, ch_req chan (com.Request), ch_rep chan (com.Reply)) {
	conn, err := ln.Accept()
	checkError(err)
	defer conn.Close()

	var reply com.Reply
	enc_worker := gob.NewEncoder(conn)
	dec_worker := gob.NewDecoder(conn)
	for req := range ch_req {
		err = enc_worker.Encode(req)
		checkError(err)

		err = dec_worker.Decode(&reply)
		checkError(err)
		ch_rep <- reply

	}
}

// Worker awaker, awakes each worker of the given file list through ssh and sets its handler for its conection
// and communication.
func awake_workers(ln net.Listener, ip, port, workers_file string, ch_req chan (com.Request), ch_rep chan (com.Reply)) {
	file, err := os.Open(workers_file)
	checkError(err)

	scanner := bufio.NewScanner(file)
	for id := 0; scanner.Scan(); id++ {
		args := strings.Split(scanner.Text(), " ")
		if args[0][0:2] != "//" && args != nil {
			args[4] += " " + ip + " " + port
			go worker_handler(ln, ch_req, ch_rep)
			go remote_ssh(id, args, false)
		}
	}
}

// Client handler, mantains Request-Reply communication with the Client.
func client_handler(ch_conn chan (net.Conn), ch_req chan (com.Request), ch_rep chan (com.Reply)) {
	var req com.Request
	for conn := range ch_conn {
		enc := gob.NewEncoder(conn)
		dec := gob.NewDecoder(conn)

		err := dec.Decode(&req)
		checkError(err)

		ch_req <- req
		go func() {
			enc.Encode(<-ch_rep)
		}()
	}
}

func main() {
	// Args= os.Args[1]: listening ip, os.Args[2]: client port, os.Args[3]: public ip, os.Args[4]: workers port, os.Args[5]: workers file
	if len(os.Args)-1 != 4 {
		fmt.Println("\n[WINDOWS]usage: master.exe listening_ip client_port workers_port worker_list_file")
		fmt.Println("[LINUX] usage: ./master listening_ip client_port workers_port worker_list_file\n")
		os.Exit(1)
	}
	// Setting up Server.
	client_ln, err := net.Listen("tcp", os.Args[1]+":"+os.Args[2])
	checkError(err)
	worker_ln, err := net.Listen("tcp", os.Args[1]+":"+os.Args[3])
	checkError(err)

	// Setting up awaker workers handlers for connection and communication.
	ch_conn, ch_req, ch_rep := make(chan (net.Conn)), make(chan (com.Request)), make(chan (com.Reply))
	go awake_workers(worker_ln, os.Args[1], os.Args[3], os.Args[4], ch_req, ch_rep)
	go client_handler(ch_conn, ch_req, ch_rep)

	// Handling Client requests.
	for {
		conn, err := client_ln.Accept()
		checkError(err)
		ch_conn <- conn
	}
}
