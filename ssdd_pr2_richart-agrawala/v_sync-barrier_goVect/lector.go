/* ****************************************************************************************
 * AUTORES: Devid Dokash (780131) y Victor()
 * ASIGNATURA: Sistemas Distribuidos
 * FECHA: Octubre de 2021
 * FICHERO: lector.go
 * DESCRIPCIÓN: Implementacion del lector del sistema Lector-Escritor.
 * ****************************************************************************************/
 package main

 import (
	"fmt"
	"os"
	"pr2/ra"
	"pr2/fm"
	"strconv"
	"time"
 )
 
 func main() {
	// os.Args[1]: me
	// os.Args[2]: local_file path
	// os.Args[3]: usersfile_ra path 
	// os.Args[4]: usersfile_fm path
	if len(os.Args) != 5 {
		fmt.Println("\nusage: lector <id> <local_file> <users_file_ra> <users_file_fm>")
		fmt.Println("\tid ............... distributed system process internal id.")
		fmt.Println("\tlocal_file ....... local copy file for the local process.")
		fmt.Println("\tusers_file_ra .... users file for ricart-agrawala message system.")
		fmt.Println("\tusers_file_fm .... users file for file manager message system.\n")
		return 
	}
	me, _ := strconv.Atoi(os.Args[1])
	ra := ra.New(me, 1, os.Args[3],"READ_RA")
	fm := fm.New(me, os.Args[2], os.Args[4],"READ_FM")

	tests := 1
	wait := 3
	time.Sleep(time.Duration(wait)*time.Second)
	for i := 0; i < tests; i++ {
		ra.PreProtocol()
		fm.Read()
		time.Sleep(time.Duration(wait)*time.Second)
		ra.PostProtocol()
	}
	ra.WaitBarrier()
}