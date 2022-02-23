/* ****************************************************************************
 * AUTHOR: Distributed Systems Department.
 * CONTRIBUTOR: Devid Dokash (780131)
 * SUBJECT: Distributed Systems.
 * DATE: January/2022
 * DESCRIPTION: RPC call with timeout implementation. Now the call while trying
 * to dial with the node will have a timeout too, implemented because of the
 * high peak of lost time while the process tries to connect to a disconnected
 * node.
 * ***************************************************************************/
package rpctimeout

import (
	"fmt"
	"net/rpc"
	"strings"
	"time"
)

// Hostport type, defines DNS or IP, as host:port.
type HostPort string

// Connection result, conformed by the client connection and an error if the
// connection does not success.
type Connect struct {
	client *rpc.Client
	err    error
}

// Returns new hostport.
func MakeHostPort(host, port string) HostPort {
	return HostPort(host + port)
}

// Returns the host of the hostport.
func (hp HostPort) Host() string {
	return string(hp[:strings.Index(string(hp), ":")])
}

// Returns the port of the hostport.
func (hp HostPort) Port() string {
	return string(hp[strings.Index(string(hp), ":")+1:])
}

// Connects via TCP to the hostport, returns a channel for sending the
// connection result.
func (hp HostPort) connect() chan Connect {
	done := make(chan Connect, 1)
	go func() {
		client, err := rpc.Dial("tcp", string(hp))
		done <- Connect{client, err}
	}()
	return done
}

// Connects via TCP to hostport before timeout and returns the rpc.Client
// correspondent to the established connection. If timeout, returns error.
func (hp HostPort) connTimeout(serviceMethod string, args interface{},
	timeout time.Duration) (*rpc.Client, error) {
	select {
	case connect := <-hp.connect():
		if connect.err != nil {
			return nil, connect.err
		}
		return connect.client, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf(
			"timeout in CallTimeout Client Dial with method: %s, args: %v",
			serviceMethod,
			args,
		)
	}
}

// Makes a RCP call with a time out, if timeout, aborts the RPC call and
// returns error
func (hp HostPort) CallTimeout(serviceMethod string, args interface{},
	reply interface{}, timeout time.Duration) error {
	client, err := hp.connTimeout(serviceMethod, args, timeout)
	if err != nil {
		return err
	}
	defer client.Close()

	done := client.Go(serviceMethod, args, reply, make(chan *rpc.Call, 1)).Done
	select {
	case call := <-done:
		return call.Error
	case <-time.After(timeout):
		return fmt.Errorf(
			"timeout in CallTimeout RPC Call with method: %s, args: %v",
			serviceMethod,
			args,
		)
	}
}

// Returns a hostport array from a string hostport array.
func StringArrayToHostPortArray(stringArray []string) (result []HostPort) {
	for _, s := range stringArray {
		result = append(result, HostPort(s))
	}
	return
}

// Returns a string hostport list from a hostport array.
func HostPortArrayToString(hostPortArray []HostPort) (result string) {
	for _, hostPort := range hostPortArray {
		result = result + " " + string(hostPort)
	}
	return
}
