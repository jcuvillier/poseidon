package config

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestRead(t *testing.T) {
	// Read config without setting config file
	{
		err := ReadInConfig()
		require.NoError(t, err)
		assert.Equal(t, 0, len(config))
	}

	// Read config from file
	{
		SetConfigFile("tstdata/ok.json")
		err := ReadInConfig()
		require.NoError(t, err)
		assert.Equal(t, 2, len(config))
	}

	// Missing file
	{
		SetConfigFile("tstdata/missing.json")
		err := ReadInConfig()
		require.Error(t, err)
	}

	// Not valid json
	{
		r := strings.NewReader(`{"keystr":"foo","keybool":f`)
		err := ReadConfig(r)
		require.Error(t, err)
	}

}

func TestGet(t *testing.T) {
	//Empty config
	v := Get("key")
	assert.Nil(t, v)

	config = map[string]interface{}{
		"keyint": 1,
		"keymap": map[string]interface{}{
			"keystr":  "str",
			"keybool": true,
		},
	}
	// Check keyint
	vInt, isInt := Get("keyint").(int)
	require.True(t, isInt)
	assert.Equal(t, 1, vInt)

	// Subpath missing
	v = Get("keyint.sub")
	assert.Nil(t, v)

	// Subpath OK
	vBool, isBool := Get("keymap.keybool").(bool)
	require.True(t, isBool)
	assert.True(t, vBool)
}

type s struct {
	KeyStr  string `json:"keystr"`
	KeyBool bool   `json:"keybool"`
	KeyEnv  string `json:"keyenv" env:"KEY_ENV"`
}

func TestUnmarshal(t *testing.T) {
	config = map[string]interface{}{
		"keyint": 1,
		"keymap": map[string]interface{}{
			"keystr":  "str",
			"keybool": true,
		},
	}

	var v1 s
	err := Unmarshal("keyint", &v1)
	require.Error(t, err)

	var v2 s
	os.Setenv("KEY_ENV", "foo")
	err = Unmarshal("keymap", &v2)
	require.NoError(t, err)
	assert.True(t, v2.KeyBool)
	assert.Equal(t, "foo", v2.KeyEnv)

	// env.Parse error
	var v3 s
	os.Setenv("KEY_ENV", "foo")
	err = Unmarshal("keynil", v3)
	require.Error(t, err)
}
