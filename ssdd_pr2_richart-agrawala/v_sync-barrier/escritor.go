/* ****************************************************************************************
 * AUTORES: Devid Dokash (780131)
 * ASIGNATURA: Sistemas Distribuidos
 * FECHA: Octubre de 2021
 * FICHERO: escritor.go
 * DESCRIPCIÓN: Código fuente correspondiente a la escritura del fichero compartido del 
 *              sistema distribuido de Lectores-Escritores.
 * ****************************************************************************************/
/* DOCUMENTACION:
 * OS PACKAGE (OpenFile, Seek, etc): https://pkg.go.dev/os
 *
 */

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
	// os.Args[1]: me,
	// os.Args[2]: localfile path
	// os.Args[3]: usersfile_ra path
	// os.Args[4]: usersfile_fm path
	// os.Args[5]: mode(INPUT, for user input or FILE, for file content as input)
	// os.Args[6]: file path (working only with FILE mode)
	var input string

	if len(os.Args) == 5 {
		input = "sistemas distribuidos esta bien, pero podria ser mejor.\n"
	} else {
		if len(os.Args) == 6 && os.Args[5] == "INPUT" {
			input, _ = fm.Input("")
		} else {
			if len(os.Args) == 7 && os.Args[5] == "FILE" {
				input, _ = fm.Input(os.Args[6])
			} else {
				fmt.Println("\nusage: escritor <id> <local_file> <users_file_ra> <users_file_fm> [{INPUT | FILE <file_path>}]")
				fmt.Println("\tid ............... distributed system process internal id.")
				fmt.Println("\tlocal_file ....... local copy file for the local process.")
				fmt.Println("\tusers_file_ra .... users file for ricart-agrawala message system.")
				fmt.Println("\tusers_file_fm .... users file for file manager message system.")
				fmt.Println("\tINPUT mode ....... flag for user input.")
				fmt.Println("\tFILE file_path ... flag for use file_path content as input.\n")
				return 
			}
		}
	}

	me, _ := strconv.Atoi(os.Args[1])
	ra := ra.New(me, 1, os.Args[3])
	fm := fm.New(me, os.Args[2], os.Args[4])
	tests := 1
	wait := 3
	time.Sleep(time.Duration(wait)*time.Second)
	for i := 0; i < tests; i++ {
		ra.PreProtocol()
		fm.Write(input, os.O_CREATE|os.O_RDWR, 0, os.SEEK_END)
		time.Sleep(time.Duration(wait)*time.Second)
		ra.PostProtocol()
	}
	ra.WaitBarrier()	 
}
 