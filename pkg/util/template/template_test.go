package template

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExpressionString(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		e := Expression{
			Text:    "foo.single",
			IsBatch: false,
		}
		assert.Equal(t, "@{foo.single}", e.String())
	})

	t.Run("batch", func(t *testing.T) {
		e := Expression{
			Text:    "foo.batch",
			IsBatch: true,
		}
		assert.Equal(t, "#{foo.batch}", e.String())
	})
}

func TestTemplateFindAll(t *testing.T) {
	in := map[string]interface{}{
		"key1": "@{args.param1}",
		"key2": "http://@{A.uri}/foo/@{A.path.foo}@{}",
		"obj": map[string]interface{}{
			"key3": "#{B}",
		},
		"key4": true,
	}

	tpl := &Template{
		input: in,
	}
	expressions := tpl.FindAll()
	assert.Len(t, expressions, 4)
	e1 := Expression{
		Text: "args.param1",
	}
	e2 := Expression{
		Text: "A.uri",
	}
	e3 := Expression{
		Text: "A.path.foo",
	}
	e4 := Expression{
		Text:    "B",
		IsBatch: true,
	}
	assert.Contains(t, expressions, e1)
	assert.Contains(t, expressions, e2)
	assert.Contains(t, expressions, e3)
	assert.Contains(t, expressions, e4)
}
