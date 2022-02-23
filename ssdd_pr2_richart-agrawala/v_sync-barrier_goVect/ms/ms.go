/* ****************************************************************************************
 * AUTORES: Devid Dokash (780131) y Victor ()
 * ASIGNATURA: Sistemas Distribuidos
 * FECHA: Octubre de 2021
 * FICHERO: ms.go
 * DESCRIPCIÓN: Implementacion de goVect en el sistema de mensajeria asincrono.
 * ****************************************************************************************
 * AUTORES: Rafael Tolosana Calasanz
 * ASIGNATURA: Sistemas Distribuidos
 * FECHA: septiembre de 2021
 * FICHERO: ms.go
 * DESCRIPCIÓN: Implementación de un sistema de mensajería asíncrono, insipirado en el Modelo Actor
 * ****************************************************************************************
 * DOCUMENTACION:
 * https://github.com/DistributedClocks/GoVector/blob/master/example/ClientServer/ClientServer.go
 * ****************************************************************************************/
package ms

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/DistributedClocks/GoVector/govec"
)

type Message interface{}

type MessageSystem struct {
	mbox   chan Message			// Canal de mensajes para el message box.
	peers  []string				// Lista de endpoints de los diferentes nodos.
	done   chan bool			// Canal para indicar fin de ejecucion.	
	me     int					// Id interno en el sistema distribuido.
	logger *govec.GoLog			// Sistema de log.
	opts   govec.GoLogOptions   // Opciones del sistema log.
}

const (
	MAXMESSAGES = 10000
)

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func parsePeers(path string) (lines []string) {
	file, err := os.Open(path)
	checkError(err)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines
}

// Pre: True
// Post: devuelve el numero de nodos.
func (ms *MessageSystem) Get_N() int {
	return len(ms.peers)
}

// Pre: pid en {1..n}, el conjunto de procesos del SD
// Post: envía el mensaje msg a pid
func (ms *MessageSystem) Send(pid int, msg Message) {
	conn, err := net.Dial("tcp", ms.peers[pid-1])
	checkError(err)
	encoder := gob.NewEncoder(conn)
	/**-->**/ ms.logger.PrepareSend("Sending message to "+strconv.Itoa(pid), nil, ms.opts)
	err = encoder.Encode(&msg)
	conn.Close()
}

// Pre: True
// Post: el mensaje msg de algún Proceso P_j se retira del mailbox y se devuelve
//		Si mailbox vacío, Receive bloquea hasta que llegue algún mensaje
func (ms *MessageSystem) Receive() (msg Message) {
	msg = <-ms.mbox
	return msg
}

func register(messageTypes []Message) {
	for _, msgTp := range messageTypes {
		gob.Register(msgTp)
	}
}

// Pre: whoIam es el pid del proceso que inicializa este ms
//		usersFile es la ruta a un fichero de texto que en cada línea contiene IP:puerto de cada participante
//		messageTypes es un slice con todos los tipos de mensajes que los procesos se pueden intercambiar a través de este ms
func New(whoIam int, usersFile string, messageTypes []Message, log_info string) (ms MessageSystem) {
	ms.me = whoIam
	ms.peers = parsePeers(usersFile)
	ms.mbox = make(chan Message, MAXMESSAGES)
	ms.done = make(chan bool)
	/**-->**/ ms.logger = govec.InitGoVector(log_info+"_"+strconv.Itoa(whoIam),log_info+"_"+strconv.Itoa(whoIam),govec.GetDefaultConfig())
	/**-->**/ ms.opts = govec.GetDefaultLogOptions()
	register(messageTypes)
	go func() {
		listener, err := net.Listen("tcp", ms.peers[ms.me-1])
		checkError(err)
		fmt.Println("Process listening at " + ms.peers[ms.me-1])
		defer close(ms.mbox)
		for {
			select {
			case <-ms.done:
				return
			default:
				conn, err := listener.Accept()
				checkError(err)
				decoder := gob.NewDecoder(conn)
				var msg Message
				err = decoder.Decode(&msg)
				/**-->**/ ms.logger.UnpackReceive("Received message",nil,nil, ms.opts)
				conn.Close()
				ms.mbox <- msg
			}
		}
	}()
	return ms
}

//Pre: True
//Post: termina la ejecución de este ms
func (ms *MessageSystem) Stop() {
	ms.done <- true
}
