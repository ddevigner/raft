/* ****************************************************************************************
 * AUTORES: Devid Dokash (780131)
 * ASIGNATURA: Sistemas Distribuidos
 * FECHA: Octubre de 2021
 * FICHERO: ricart-agrawala.go
 * DESCRIPCIÓN: Implementacion completa del algoritmo de Ricart-Agrawala Generalizado en Go
 * ****************************************************************************************
 * AUTORES: Rafael Tolosana Calasanz
 * ASIGNATURA: Sistemas Distribuidos
 * FECHA: septiembre de 2021
 * FICHERO: ricart-agrawala.go
 * DESCRIPCIÓN: Implementación del algoritmo de Ricart-Agrawala Generalizado en Go
 * ****************************************************************************************
 * DOCUMENTACION
 * ****************************************************************************************/
 package ra

import (
	"sync"
	"pr2/ms"
)

type Request struct {
	Clock int
	Pid   int
	CoOp  int
}

type Reply struct{}

/** Mensaje de tipo Fin. Indica estado de finalizacion de un nodo. **/
type End struct{}

/** Vector de exclusion **/
var exclude [2][2]bool = [2][2]bool{{false,true},{true,true}}

/** Estructura de los datos argumentos necesarios que implementa el algoritmo de Ricart-Agrawala **/
type RASharedDB struct {
	Me	        int	// Id interno en el sistema distribuido.
	CoOp        int	// Codigo de operacion que implementa.
	N           int	// Numero de nodos total.
	RemainNodes int // Nodos restantes por finalizar.
	OurSeqNum   int	// Numero local de reloj.
	HigSeqNum   int	// Numero global de reloj.
	OutRepCnt   int	// Numero total de respuestas para entrar en sc.
	ReqCS       bool	// Indicador de que se pretende entrar en sc.
	RepDefd     []int	// Id de los nodos cuya respuesta ha sido postergada.
	ms          *ms.MessageSystem	// Sistema de mensajes asincrono modelo actor.
	done        chan bool	// Canal para indicar fin de ejecucion.	
	chrep       chan bool	// Canal para indicar que todas las respuestas han sido recibidas.
	exit        chan bool   // Canal para indicar fin de ejecucion de todos los nodos
	Mutex       sync.Mutex	// Mutex para proteger concurrencia sobre las variables
}

func New(me int, coOp int, usersFile string) *RASharedDB {
	messageTypes := []ms.Message{Request{}, Reply{}, End{}}
	msgs := ms.New(me, usersFile, messageTypes)
	ra := RASharedDB{me, coOp, msgs.Get_N(), msgs.Get_N()-1,0, 0, 0, false, []int{}, &msgs, make(chan bool), make(chan bool), make(chan bool), sync.Mutex{}}

	go func() {
		for {
			select {
			case <-ra.done:
				return
			default:
				msg := ra.ms.Receive()
				switch req := msg.(type) {
				case Request:
					var defer_it bool
					if ra.HigSeqNum < req.Clock {
						ra.HigSeqNum = req.Clock
					}

					ra.Mutex.Lock()
					defer_it = (ra.ReqCS && ((req.Clock > ra.OurSeqNum) || (req.Clock == ra.OurSeqNum && req.Pid > me))) && exclude[ra.CoOp][req.CoOp]
					ra.Mutex.Unlock()

					if defer_it {
						ra.RepDefd = append(ra.RepDefd, req.Pid)
					} else {
						ra.ms.Send(req.Pid, Reply{})
					}
				case Reply:
					if ra.OutRepCnt--; ra.OutRepCnt == 0 {
						ra.chrep <- true
					}
				case End:
					if ra.RemainNodes--; ra.RemainNodes == 0 {
						ra.exit <- true
					}
				}
			}
		}
	}()
	return &ra
}

//Pre: Verdad
//Post: Realiza  el  PreProtocol  para el  algoritmo de
//      Ricart-Agrawala Generalizado
func (ra *RASharedDB) PreProtocol() {
	// TODO completar
	ra.Mutex.Lock()
	ra.ReqCS = true
	ra.OurSeqNum = ra.HigSeqNum + 1
	ra.Mutex.Unlock()

	ra.OutRepCnt = ra.N - 1
	for i := 1; i <= ra.N; i++ {
		if ra.Me != i {
			ra.ms.Send(i, Request{ra.OurSeqNum, ra.Me, ra.CoOp})
		}
	}
	<-ra.chrep
}

//Pre: Verdad
//Post: Realiza  el  PostProtocol  para el  algoritmo de
//      Ricart-Agrawala Generalizado
func (ra *RASharedDB) PostProtocol() {
	// TODO completar
	ra.ReqCS = false
	for _, v := range ra.RepDefd {
		ra.ms.Send(v,Reply{})
	}
	ra.RepDefd = []int{}	
}

//Pre: Verdad
//Post: Realiza la espera de la barrera de sincronizacion 
//  	para el algoritmo de Ricart-Agrawala Generalizado.
func (ra *RASharedDB) WaitBarrier(){
	for i := 1; i <= ra.N; i++ {
		if ra.Me != i {
			ra.ms.Send(i,End{})
		}
	}
	<-ra.exit
}


func (ra *RASharedDB) Stop() {
	ra.ms.Stop()
	ra.done <- true
}
