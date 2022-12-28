package main

import (
	"flag"
	"fmt"
	"os"

	resp "github.com/Eslam-Nawara/bitcask/pkg/respserver"
)

func main() {
	pathPtr := flag.String("d", "datastore", "specify the desired datastore path")
	port := flag.Int("p", 6379, "specify the desired server port")
	flag.Parse()

	server, err := resp.New(*pathPtr, fmt.Sprintf(":%d", *port))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer server.Close()

	err = server.ListenAndServe()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
