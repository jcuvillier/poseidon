package maps

import (
	"strings"

	"github.com/mitchellh/mapstructure"
)

// Get returns the value for the given key
func Get(m interface{}, key string) interface{} {
	var obj interface{} = m
	var val interface{} = nil

	parts := strings.Split(key, ".")
	for _, p := range parts {
		if v, ok := obj.(map[string]interface{}); ok {
			obj = v[p]
			val = obj
		} else {
			return nil
		}
	}
	return val
}

// Decode takes an input structure and uses reflection to translate it to the output structure. output must be a pointer to a map or struct.
func Decode(in, out interface{}) error {
	return mapstructure.Decode(in, out)
}
