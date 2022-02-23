/* ****************************************************************************************
 * AUTORES: Devid Dokash (780131)
 * ASIGNATURA: Sistemas Distribuidos
 * FECHA: Octubre de 2021
 * FICHERO: worker.go
 * DESCRIPCIÓN: Codigo fuente del Worker de la arquitectura Cliente-Master-Worker
 * ****************************************************************************************/
package main

import (
	"encoding/gob"
	"fmt"
	"net"
	"os"

	"practica-1/com"
)

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

func main() {

	// Args= os.Args[1]: ip, os.Args[2]: port
	if len(os.Args)-1 != 2 {
		fmt.Println("\n[WINDOWS]usage: worker.exe master_ip master_port")
		fmt.Println("[LINUX] usage: ./worker master_ip master_port")
		os.Exit(1)
	}
	// Resolving received address.
	tcpAddr, err := net.ResolveTCPAddr("tcp", os.Args[1]+":"+os.Args[2])
	checkError(err)

	// Connecting to server.
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkError(err)

	enc, dec := gob.NewEncoder(conn), gob.NewDecoder(conn)
	var req com.Request
	for {
		dec.Decode(&req)

		// Calculates the corresponding primes for the received interval
		enc.Encode(com.Reply{req.Id, FindPrimes(req.Interval)})
	}
}
