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
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"time"

	"pr3/com"
	"pr3/ssh_conn"
)

type Peer struct {
	endpoint 	string				// Endpoint del nodo.
	cmd		 	string				// Comando especifico para encender la maquina.
	ssh_info	*ssh_conn.SSH_STATE	// Informacion necesaria para establecer la conexion ssh.
	status		bool				// False: apagado, True: encendido.
}

// --> PRIMESIMPL STRUCT
type PrimesImpl struct {
	connRetries				int
	connWaitTime			int
	connWaitTimeGrowFactor	float64
	args					chan(Args)
	ch_err					chan(error)
}

type Args struct {
	interval	com.TPInterval
	primeList	*[]int
}

func (p *PrimesImpl) FindPrimes(interval com.TPInterval, primeList *[]int) error {
	p.args <- Args{interval, primeList}
	err := <-p.ch_err
	
	if err != nil {
		for i := 0; i < 5 && err != nil; i++ {
			p.args <- Args{interval, primeList}
			err = <-p.ch_err
		}
	}
	return err
}

func (p *PrimesImpl) connection_handler(peer *Peer){
	time.Sleep(time.Duration(p.connWaitTime) * time.Second)
	dial, err := rpc.DialHTTP("tcp", peer.endpoint)

	for i := 0; i < p.connRetries && err != nil; i++ {
		time.Sleep(time.Duration((float64(p.connWaitTime) + (float64(p.connWaitTime) * (p.connWaitTimeGrowFactor * float64(i))))) * time.Second)
		dial, err = rpc.DialHTTP("tcp", peer.endpoint)
	}
	if err != nil {
		return
	}
	peer.status = true
	for args := range p.args {
		err = dial.Call("PrimesImpl.FindPrimes", args.interval, args.primeList)
		if err != nil {
			p.ch_err <- err
		}
		p.ch_err <- nil
	}
}

/** PRIVATE FUNCTIONS **/
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func parse_peers(peers_file string) (peers []*Peer, err error){
	file, err := os.Open(peers_file )
	if err != nil {
		return
	}

	for scanner := bufio.NewScanner(file); scanner.Scan(); {
		args := strings.Split(scanner.Text(), " ")
		flags := func() []bool {
			f := []bool{}
			for _, v := range []string{args[4],args[5],args[6]} {
				if v == "1" {
					f = append(f,true)
				} else {
					f = append(f,false)
				}
			}
			return f
		}()
		sshci, err := ssh_conn.New(args[0], args[1], args[3], args[7], flags)
		if err != nil {
			return nil, err
		}
		peers = append(peers, &Peer{
			endpoint:		args[1]+":"+args[2], 
			cmd:			args[8]+" "+args[1]+":"+args[2],
			ssh_info:		sshci,
			status:			false,
		})
	}
	return
}

func next_peer(peers []*Peer){
	for i, v := range peers {
		if v.status {
			return i
		}
	}
}

func main() {
	// Args= os.Args[1]: ip:port (master), os.Args[2]: workers file path.
	if len(os.Args)-1 != 2 {
		fmt.Println("\nUsage: go run master.go <ip:port> <workers_file_path>")
		os.Exit(1)
	}

	network, err := parse_peers(os.Args[2])
	checkError(err)

	primesImpl := &PrimesImpl{
		connRetries:				5,
		connWaitTime:				11,
		connWaitTimeGrowFactor:		0.09,
		args:						make(chan(Args)), 
		ch_err:						make(chan(error)),
	}
	for _, v := range network {
		go ssh_conn.Run_SSH_cmd(v.ssh_info, v.cmd)
		go primesImpl.connection_handler(v)
	}

	rpc.Register(primesImpl)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", os.Args[1])
	checkError(err)
	http.Serve(l, nil)
}
