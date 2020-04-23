package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetInputDependencies(t *testing.T) {
	t.Run("asd", func(t *testing.T) {
		input := map[string]interface{}{
			"str":  "#{args:str}",
			"num":  1,
			"bool": true,
			"obj": map[string]interface{}{
				"path": "http://${args:ref.uri}/toto/${nodeA}",
			},
		}
		n := NodeSpec{
			Input: input,
		}
		deps := n.GetInputDependencies()
		d1 := InputDependency{
			OriginalString: "#{args:str}",
			Name:           "args",
			Path:           "str",
			IsBatch:        true,
		}
		d2 := InputDependency{
			OriginalString: "${args:ref.uri}",
			Name:           "args",
			Path:           "ref.uri",
			IsBatch:        false,
		}
		d3 := InputDependency{
			OriginalString: "${nodeA}",
			Name:           "nodeA",
		}
		assert.Equal(t, 3, len(deps))
		assert.Contains(t, deps, d1)
		assert.Contains(t, deps, d2)
		assert.Contains(t, deps, d3)
	})
}
