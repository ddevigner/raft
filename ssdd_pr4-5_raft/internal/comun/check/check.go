/* ****************************************************************************
 * AUTHOR: Distributed Systems Department.
 * SUBJECT: Distributed Systems.
 * DATE: January/2022
 * DESCRIPTION: errors check functions.
 * ***************************************************************************/
package check

import (
	"fmt"
	"os"
)

// If error, exits the program. Can print additional info.
func CheckError(err error, comment string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "In: %s, Fatal error: %s\n", comment,
			err.Error())
		os.Exit(1)
	}
}
