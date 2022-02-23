/* ****************************************************************************************
 * AUTORES: Devid Dokash (780131)
 * ASIGNATURA: Sistemas Distribuidos
 * FECHA: Octubre de 2021
 * FICHERO: fm.go
 * DESCRIPCIÓN: Código fuente correspondiente al gestor de ficheros del sistema distribuido.
 * ****************************************************************************************
 * DOCUMENTACION:
 * OS PACKAGE (OpenFile, Seek, etc): https://pkg.go.dev/os
 * ****************************************************************************************/
package fm

import (
	"fmt"
	"io/ioutil"
	"bufio"
	"os"
	"pr2/ms"
)

type FileContentUpdate struct {
	Content string
	Flag 	int
	Offset  int64
	Whence  int 
}

type FileManager struct {
	me         int
	N          int
	ms         *ms.MessageSystem
	localFile  string
	done       chan bool
}

func New(me int, localFile string, usersFile, log_info string) *FileManager {
	messageTypes := []ms.Message{FileContentUpdate{}}
	msgs := ms.New(me, usersFile, messageTypes, log_info)
	fm := FileManager{me, msgs.Get_N(), &msgs, localFile, make(chan bool)}

	go func(){
		for {
			select {
			case <- fm.done:
				return
			default:
				msg := fm.ms.Receive().(FileContentUpdate)
				f, _ := os.OpenFile(fm.localFile, msg.Flag, 0700);
				f.Seek(msg.Offset, msg.Whence)
				f.WriteString(msg.Content)
			}
		}
	}()
	return &fm
}

func (fm *FileManager) Read() error {
	content, err := ioutil.ReadFile(fm.localFile);
	if err != nil {
		return err
	}
	fmt.Println(string(content))
	return nil
}

func Input(file string) (content string, err error) {
	if file == "" {
		fmt.Print("> ")
		reader := bufio.NewReader(os.Stdin)
		content, err = reader.ReadString('\n')
		return
	}
	content_b, err := ioutil.ReadFile(file)
	content = string(content_b)
	return
}

func (fm *FileManager) Write(new_content string, flag int, offset int64, whence int) error {
	f, err := os.OpenFile(fm.localFile, flag, 0700);
	if err != nil {
		return err
	}
	if _, err = f.Seek(offset, whence); err != nil {
		return err
	}
	if _, err = f.WriteString(new_content); err != nil {
		return err
	}
	for i := 1; i <= fm.N; i++ {
		if i != fm.me {
			fm.ms.Send(i,FileContentUpdate{new_content, flag, offset, whence})
		}
	}
	return nil
}

func (fm *FileManager) Stop(){
	fm.ms.Stop()
	fm.done <- true
}