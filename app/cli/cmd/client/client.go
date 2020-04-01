package client

import "poseidon/pkg/client"

// New returns a new poseidon client
func New() (client.Client, error) {
	return client.NewClient("http://127.0.0.1:8080")
}
