/* ****************************************************************************
 * AUTHOR: Distributed Systems Department.
 * CONTRIBUTOR: Devid Dokash (780131)
 * SUBJECT: Distributed Systems.
 * DATE: January/2022
 * DESCRIPTION: ssh connection and remote command execution. Now the functions
 * have the option of displaying the commands output and the execution errors,
 * check the verbose and stdout constants.
 * ***************************************************************************/
package despliegue

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

const (
	// If true, prints the ssh function error if exists.
	verbose = false
	// If true, displays through stdout the remote command output.
	stdout = false
)

// Gets ssh host key from a host.
func getHostKey(host string) ssh.PublicKey {
	// parse OpenSSH known_hosts file
	// ssh or use ssh-keyscan to get initial key
	home, _ := os.UserHomeDir()
	file, err := os.Open(filepath.Join(home, ".ssh", "known_hosts"))
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var hostKey ssh.PublicKey
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), " ")
		if len(fields) != 3 {
			continue
		}
		if strings.Contains(fields[0], host) {
			var err error
			hostKey, _, _, _, err = ssh.ParseAuthorizedKey(scanner.Bytes())
			if err != nil {
				log.Fatalln(time.Now(), "SSH connection - error in parsing",
					"authorized key ", fields[2], ": ", err)
			}
			break
		}
	}
	if hostKey == nil {
		log.Fatalln(time.Now(), "SSH connection - no hostkey found for", host)
	}

	return hostKey
}

// Executes a command remotely via ssh.
func executeCmd(cmd, hostname string, config *ssh.ClientConfig) string {
	conn, err := ssh.Dial("tcp", hostname+":22", config)
	if err != nil {
		log.Fatalln(time.Now(), "SSH connection - dial error:", err)
	}
	defer conn.Close()

	session, err := conn.NewSession()
	if err != nil {
		log.Fatalln(time.Now(), "SSH connection - new session error:", err)
	}
	defer session.Close()

	stdoutBuf := bytes.Buffer{}
	session.Stdout = &stdoutBuf
	session.Stderr = &stdoutBuf
	if stdout {
		session.Stdout = os.Stdout
		session.Stderr = os.Stderr
	}

	err = session.Run(cmd)
	if err != nil {
		log.Fatalln(time.Now(), "SSH connection - run command error:", err)
	}

	if stdout {
		return hostname
	} else {
		return "Ended ssh session: " + hostname + ": \n" + stdoutBuf.String()
	}
}

// Returns a new ssh client config for a new session connection.
func buildSSHConfig(signer ssh.Signer,
	hostKey ssh.PublicKey) *ssh.ClientConfig {

	return &ssh.ClientConfig{
		User: os.Getenv("LOGNAME"),
		// Authentication method.
		Auth: []ssh.AuthMethod{
			// PublicKeys method for remote authentication.
			ssh.PublicKeys(signer),
		},
		// Verify host public key.
		// HostKeyCallback: ssh.FixedHostKey(hostKey),
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // Non-production only
		// Optional TCP Connection timeout.
		Timeout: 5 * time.Second,
	}
}

// Execute a command into the given node.
func execOneNode(hostname string, results chan<- string,
	signer ssh.Signer, cmd string) {
	// Get host public key
	// ssh_config must have option "HashKnownHosts no".
	hostKey := getHostKey(hostname)
	config := buildSSHConfig(signer, hostKey)

	if verbose {
		fmt.Println(time.Now(), "SSH connection - running command: ", cmd)
		fmt.Println(time.Now(), "SSH connection - hostname: ", hostname)
	}

	results <- executeCmd(cmd, hostname, config)
}

// Executes a command in multiple hosts.
func ExecMultipleNodes(cmd string,
	hosts []string,
	results chan<- string,
	privKeyFile string) {

	//Read private key file for user
	home, _ := os.UserHomeDir()
	pkey, err := ioutil.ReadFile(
		filepath.Join(home, ".ssh", privKeyFile))
	if err != nil {
		log.Fatalln(time.Now(),
			"- SSH connection - private key file read error:", err)
	}

	// Create the Signer for this private key.
	signer, err := ssh.ParsePrivateKey(pkey)
	if err != nil {
		log.Fatalln(time.Now(),
			"SSH connection - private key parse error:", err)
	}

	for _, hostname := range hosts {
		go execOneNode(hostname, results, signer, cmd)
	}
}
