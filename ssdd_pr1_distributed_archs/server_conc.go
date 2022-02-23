/*
* AUTOR: Víctor Marcuello Baquero (741278)
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2021
* FICHERO: servidor.go
* DESCRIPCIÓN: contiene la funcionalidad esencial para realizar el servidor
			   correspondiente a la arquitectura de cliente-servidor concurrente
*/
package main

import (
	"encoding/gob"
	"fmt"
	"net"
	"os"
	//"io"
	"com"
)

/*const(
	CONN_TYPE = "tcp"
	CONN_HOST = "127:0:0:1"
	CONN_PORT = "30000"
)*/

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

// PRE: verdad
// POST: IsPrime devuelve verdad si n es primo y falso en caso contrario
func IsPrime(n int) (foundDivisor bool) {
	foundDivisor = false
	for i := 2; (i < n) && !foundDivisor; i++ {
		foundDivisor = (n%i == 0)
	}
	return !foundDivisor
}

// PRE: interval.A < interval.B
// POST: FindPrimes devuelve todos los números primos comprendidos en el
// 		intervalo [interval.A, interval.B]
func FindPrimes(interval com.TPInterval) (primes []int) {
	for i := interval.A; i <= interval.B; i++ {
		if IsPrime(i) {
			primes = append(primes, i)
		}
	}
	return primes
}

func clientHandler(conn net.Conn){
	var request com.Request //Variable que almacena la petición envíada por el cliente
       	
       	encoder := gob.NewEncoder(conn)	
		decoder := gob.NewDecoder(conn)
		
		err := decoder.Decode(&request)
		checkError(err)	
		
		interval := request.Interval
		
		primes := FindPrimes(interval)
	
		//fmt.Printf("Sending : %+v\n", primes)
	
		reply := com.Reply{request.Id,primes}
		
		err = encoder.Encode(reply)
		checkError(err)
		
		conn.Close()
}

func main() {

	listener, err := net.Listen("tcp", os.Args[1]+":"+os.Args[2])
	checkError(err)


	for{
		conn, err := listener.Accept()
		defer conn.Close()
		checkError(err)
	
		go clientHandler(conn)
    	
    }
}
