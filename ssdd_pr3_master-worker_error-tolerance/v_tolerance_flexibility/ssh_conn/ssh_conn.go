/* ****************************************************************************************
 * AUTORES: Devid Dokash (780131)
 * ASIGNATURA: Sistemas Distribuidos
 * FECHA: Octubre de 2021
 * FICHERO: master.go
 * DESCRIPCIÓN: Codigo fuente del Master de la arquitectura Cliente-Master-Worker
 * ****************************************************************************************/
package ssh_conn

import (
	"io/ioutil"
	"net"
	"os"
	"golang.org/x/crypto/ssh"
)

type SSH_STATE struct {
	ip				string				// SSH connection ip.
	port			string				// SSH connection port.
	config			*ssh.ClientConfig	// Client configuration.
	flags 			[]bool 				// 0: STDIN, 1: STDOUT, 2: STDERR
}

func New(user, ip, port, priv_key_path string, flags []bool) (*SSH_STATE, error) {
	priv_key, err := ioutil.ReadFile(priv_key_path)
	if err != nil {
		return nil, err 
	}
	sign, err := ssh.ParsePrivateKey(priv_key)
	if err != nil {
		return nil, err
	}
	return &SSH_STATE{
		ip: 			ip,
		port: 			port,
		config: 		&ssh.ClientConfig{
			User: 				user,
			HostKeyCallback: 	ssh.InsecureIgnoreHostKey(),
			Auth:				[]ssh.AuthMethod{ssh.PublicKeys(sign)},
		},
		flags:			flags,
	}, nil
}

/* Connects via ssh and runs the specified command. */
func Run_SSH_cmd(state *SSH_STATE, cmd string) (err error) {
	// Connection via TCP throught.
	ssh_client, err := ssh.Dial("tcp", net.JoinHostPort(state.ip, state.port), state.config)
	if err != nil {
		return
	}
	defer ssh_client.Close()

	// Openning a new ssh session for the ssh client.
	ssh_session, err := ssh_client.NewSession()
	if err != nil {
		return
	}
	defer ssh_session.Close()

	// Setting up of standard output, input and error output.
	if state.flags[0] {
		ssh_session.Stdin = os.Stdin
	}
	if state.flags[1] {
		ssh_session.Stdout = os.Stdout
	}
	if state.flags[2] {
		ssh_session.Stderr = os.Stderr
	}

	// Run the remote command
	err = ssh_session.Run(cmd)
	if err != nil {
		return
	}
	return
}

