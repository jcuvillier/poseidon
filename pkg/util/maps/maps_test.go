package maps

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
	m := map[string]interface{}{
		"str": "foo",
		"num": 1,
		"obj": map[string]interface{}{
			"bool":  false,
			"array": []string{"toto", "tutu", "tata"},
		},
	}
	str := Get(m, "str")
	assert.Equal(t, "foo", str)

	bool := Get(m, "obj.bool")
	assert.Equal(t, false, bool)

	null := Get(m, "obj.bool.null")
	assert.Nil(t, null)
}
