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
	"strconv"
	"strings"
	"sync"
	"time"
 
	"pr3/com"
	"pr3/ssh_conn"
	"github.com/inancgumus/screen"
)
 
/*******************************************************************************************************/
/** FIND PRIMES **/
type PrimesImpl struct {
	available_workers	chan(*Worker)
	off_workers			chan(*Worker)
	petitions			chan(bool)
	mtx					sync.Mutex
}
 
func isPrime(n int) (foundDivisor bool) {
	foundDivisor = false
	for i := 2; (i < n) && !foundDivisor; i++ {
		foundDivisor = (n%i == 0)
	}
	return !foundDivisor
}
 
// PRE: verdad
// POST: IsPrime devuelve verdad si n es primo y falso en caso contrario
func findPrimes(interval com.TPInterval) (primes []int) {
	for i := interval.A; i <= interval.B; i++ {
		if isPrime(i) {
			primes = append(primes, i)
		}
	}
	return primes
}

/** VERSION 1. EL MASTER NO RESPALDA A LOS WORKERS. PROCURA QUE ELLOS SE ENCARGUEN DE HACER TODO EL TRABAJO **/ /*
func (p *PrimesImpl) FindPrimes(interval com.TPInterval, primeList *[]int) error {
	 p.petitions <- true
	 case wrk := <-p.available_workers:
		 fmt.Println("I took a worker: ", *wrk)
	 
		 enough := make(chan bool)
		 go func(){
			 select {
			 case <-time.After(2 * time.Second):
				 fmt.Println("I kill the worker. 2 seconds passes.")
				 wrk.dial.Call("PrimesImpl.Stop", 0, nil)
				 <- enough
			 case <- enough:
				 fmt.Println("My master has finished.")
			 }
		 }()
		 err := wrk.dial.Call("PrimesImpl.FindPrimes", interval, primeList)
		 // for err != nil {
		 if err != nil {
			 enough <- true
			 p.off_workers <- wrk
			 wrk = <-p.available_workers
			 go func(){
				 select {
				 case <-time.After(2 * time.Second):
					 fmt.Println("I kill the worker. 2 seconds passes.")
					 wrk.dial.Call("PrimesImpl.Stop", 0, nil)
					 <- enough
				 case <- enough:
					 fmt.Println("My master has finished.")
				 }
			 }()
			 wrk.dial.Call("PrimesImpl.FindPrimes", interval, primeList)
			 
		 }
		 enough <- true
		 p.available_workers <- wrk
		 fmt.Println("I left a worker: ", *wrk)
	 }
	 <-p.petitions
	 return nil
}*/
 
/** VERSION 2. EL MASTER RESPALDA A LOS WORKERS. SI NO PUEDEN, EL MISMO SE ENCARGARA DE HACERLO. **/
func (p *PrimesImpl) FindPrimes(interval com.TPInterval, primeList *[]int) error {
	p.petitions <- true
	select {
	case <-time.After(1700 * time.Millisecond):
		*primeList = findPrimes(interval)
	case wrk := <-p.available_workers:
		//fmt.Println("I took a worker: ", *wrk)
		enough := make(chan bool)
		go func(){
			select {
			case <-time.After(2250 * time.Millisecond):
				//fmt.Println("I kill the worker. 2 seconds passes.")
				wrk.dial.Call("PrimesImpl.Stop", 0, nil)
				<- enough
			case <- enough:
				//fmt.Println("My master has finished.")
			}
		}()
		err := wrk.dial.Call("PrimesImpl.FindPrimes", interval, primeList)
		enough <- true
		if err != nil {
			p.off_workers <- wrk
			*primeList = findPrimes(interval)
		} else {
			p.available_workers <- wrk
		}
		//fmt.Println("I left a worker: ", *wrk)
	}
	<-p.petitions
	return nil
 }
 
 /*******************************************************************************************************/
 /** WORKER **/
 type Worker struct {
	dial		*rpc.Client
	working 	bool
	endpoint	string
	cmd			string
	ssh_info	*ssh_conn.SSH_STATE
 }
 
 func init_worker(p *PrimesImpl) {
	wrk := <- p.off_workers
	go ssh_conn.Run_SSH_cmd(wrk.ssh_info, wrk.cmd)

	time.Sleep(11 * time.Second)
	dial, err := rpc.DialHTTP("tcp", wrk.endpoint)
	if wrk.dial = dial; err == nil {
		p.available_workers <- wrk
	} else {
		p.off_workers <- wrk
	}
 }
 
 /*******************************************************************************************************/
 /** MASTER **/
 // PRE: true
 // POST: checks if there is any error, if it is, prints the error and exits from execution.
 func checkError(err error) {
	 if err != nil {
		 fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		 os.Exit(1)
	 }
 }
 
 // PRE: true
 // POST: Returns a workers set parsed from <workers_file> file and an error if exists.
 func parse_peers(workers_file string) (workers []*Worker, err error){
	 file, err := os.Open(workers_file)
	 if err != nil {
		 return
	 }
 
	 for scanner := bufio.NewScanner(file); scanner.Scan(); {
		 args := strings.Split(scanner.Text(), " ")
 
		 flags := []bool{}
		 for _, v := range []string{args[4],args[5],args[6]} {
			 if v == "1" {
				 flags = append(flags,true)
			 } else {
				 flags = append(flags,false)
			 }
		 }
		 sshci, err := ssh_conn.New(args[0], args[1], args[3], args[7], flags)
		 if err != nil {
			 return nil, err
		 }
 
		 workers = append(workers, &Worker{
			 endpoint:		args[1]+":"+args[2], 
			 cmd:			args[8]+" "+args[1]+":"+args[2],
			 ssh_info:		sshci,
		 })
	 }
	 return
}

func conn_handler_timer(p *PrimesImpl, gap int, restart chan int, handler func(*PrimesImpl)){
	fmt.Println("ALARM SET: ", gap, "s")

	timeOut := time.NewTicker(time.Duration(gap) * time.Second) 
	for {
		select {
		case <-timeOut.C:
			handler(p)
		case new_gap:= <-restart:
			timeOut = time.NewTicker(time.Duration(new_gap) * time.Second)
		}
	}
}

// PRE: true
// POST: master handler, in charge of connection and system charge.
func master_handler(p *PrimesImpl, n int) {
	//petitions, available, off := 0, 0, 0
	// Each 10 seconds, check if workers are not needed.
	restart := make(chan int)
	go conn_handler_timer(p, 60, restart, func(p *PrimesImpl){
		if len(p.petitions) < len(p.available_workers) && len(p.available_workers) > 0 {
			fmt.Println("KILLING WORKER.")
				wrk := <-p.available_workers
				wrk.dial.Call("PrimesImpl.Stop", 0, nil)
				p.off_workers <- wrk
		}
		if len(p.petitions) == 0 && len(p.off_workers) == cap(p.off_workers) {
			fmt.Println("BYE BYE")
			os.Exit(0)
		}
	})

	go conn_handler_timer(p, 1, nil, func(p *PrimesImpl){
		screen.Clear()
		screen.MoveTopLeft()
		p.mtx.Lock()
		fmt.Println("*****************************************************************************")
		fmt.Println("* Petitions: ", len(p.petitions))
		fmt.Println("* Total workers: ", cap(p.available_workers))
		fmt.Println("* Other (Working, connecting, shutting down... ): ", cap(p.available_workers) - (len(p.available_workers) + len(p.off_workers)))
		fmt.Println("* Available workers: ", len(p.available_workers))
		fmt.Println("* Inactive workers: ", len(p.off_workers))
		fmt.Println("*****************************************************************************")
		p.mtx.Unlock()
	})

	for {
		p.mtx.Lock()
		if len(p.petitions) > len(p.available_workers) && len(p.off_workers) > 0 {
			p.mtx.Unlock()
			restart <- 30
			init_worker(p)
		} else {
			p.mtx.Unlock()
		}
	}
}
 
func main() {
	// Args= os.Args[1]: ip:port (master), os.Args[2]: n, os.Args[3]: workers_file_path.
	if len(os.Args)-1 != 3 {
		fmt.Println("\nussage: go run master.go <ip:port> <n> <workers_file_path>")
		os.Exit(1)
	}
 
	workers, err := parse_peers(os.Args[3])
	if checkError(err); len(workers) == 0 {
		fmt.Println("\nerror: no workers to use, what am I supposed to do without any workers???")
		os.Exit(1)
	}
 
	n, err := strconv.Atoi(os.Args[2])
	if checkError(err); n > len(workers) {
		//fmt.Println("\nwarning: did you just declare more workers than those that you have? Just... why? okey, let me change it...")
		n = len(workers)
	}
 
	p := &PrimesImpl{make(chan *Worker, len(workers)), make(chan *Worker, len(workers)), make(chan bool, 1024), sync.Mutex{}}
	for _, v := range workers {
		p.off_workers <- v
	}
	for i := 0; i < n; i++ {
		go init_worker(p)
	}

	time.Sleep(11 * time.Second)
	go master_handler(p, n)
	rpc.Register(p)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", os.Args[1])
	checkError(err)
	http.Serve(l, nil)
}
 