package config

import (
	"encoding/json"
	"io"
	"os"
	"strings"

	"github.com/caarlos0/env/v6"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

var (
	config     = make(map[string]interface{})
	configFile string
)

// SetConfigFile sets the config file path to be read
func SetConfigFile(path string) {
	configFile = path
}

// ReadInConfig reads the config file previously set
// If no config file was set, does nothing
func ReadInConfig() error {
	if configFile == "" {
		//No config file set, just return
		return nil
	}

	f, err := os.Open(configFile)
	if err != nil {
		return errors.Wrapf(err, "cannot open file %s", configFile)
	}
	defer f.Close()

	return ReadConfig(f)
}

// ReadConfig read config from the given reader
func ReadConfig(in io.Reader) error {
	if err := json.NewDecoder(in).Decode(&config); err != nil {
		return errors.Wrap(err, "cannot decode config")
	}
	return nil
}

// Get returns the value for the given key
func Get(key string) interface{} {
	var obj interface{} = config
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

// Unmarshal parses the config data for the given key and stores the result in the value pointed to by v.
func Unmarshal(key string, v interface{}) error {
	in := Get(key)
	//Decode from config data
	if in != nil {
		if err := mapstructure.Decode(in, v); err != nil {
			return errors.Wrapf(err, "cannot decode config for key %s", key)
		}
	}
	// Parse env variables
	if err := env.Parse(v); err != nil {
		return errors.Wrap(err, "cannot parse env")
	}
	return nil
}
