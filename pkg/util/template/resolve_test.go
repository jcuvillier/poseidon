package template

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type obj struct {
	Str string `json:"str"`
	Num int    `json:"num"`
}

var (
	resolver = func(e Expression) (interface{}, error) {
		if e.Text == "" {
			return "", nil
		}
		i := strings.Index(e.Text, ".")
		typ := e.Text[:i]
		val := e.Text[i+1:]
		switch typ {
		case "Str":
			return val, nil
		case "Num":
			return strconv.Atoi(val)
		case "Bool":
			return strconv.ParseBool(val)
		case "Arr":
			return strings.Split(val, "."), nil
		case "Obj":
			return obj{
				Str: val,
				Num: 1,
			}, nil
		case "Err":
			return nil, errors.New(val)
		}
		return nil, nil
	}
)

func TestTemplateResolve(t *testing.T) {
	t.Run("nodep", func(t *testing.T) {
		tpl := Template{
			input: map[string]interface{}{
				"key1": "foo",
				"key2": 456,
			},
		}
		res, err := tpl.Resolve(resolver)
		require.NoError(t, err)
		assert.Equal(t, tpl.input, res)
	})

	t.Run("regular_nobatch", func(t *testing.T) {
		var in interface{}
		data := []byte(`{
			"key1": "@{Obj.foo}",
			"key2": "prefix_@{Str.foo}_@{Num.5}",
			"obj": {
				"key3": "@{Bool.false}"
			},
			"array": ["toto", "@{Str.tutu}", "tata"],
			"key4":  true
		}`)
		err := json.Unmarshal(data, &in)
		require.NoError(t, err)
		tpl := Template{in}

		res, err := tpl.Resolve(resolver)
		require.NoError(t, err)
		expected := `{
			"key1": {
				"str": "foo",
				"num": 1
			},
			"key2": "prefix_foo_5",
			"obj": {
				"key3": false
			},
			"array": ["toto", "tutu", "tata"],
			"key4":  true
		}`
		r, err := json.Marshal(res)
		require.NoError(t, err)
		assert.JSONEq(t, expected, string(r))
	})

	t.Run("regular_batch", func(t *testing.T) {
		var in interface{}
		data := []byte(`{
			"key1": "@{Bool.true}",
			"key2": "#{Arr.toto.tutu.tata}"
		}`)
		err := json.Unmarshal(data, &in)
		require.NoError(t, err)
		tpl := Template{in}

		res, err := tpl.Resolve(resolver)
		require.NoError(t, err)
		expected := `[
			{"key1": true, "key2": "toto"},
			{"key1": true, "key2": "tutu"},
			{"key1": true, "key2": "tata"}
		]`
		r, err := json.Marshal(res)
		require.NoError(t, err)
		assert.JSONEq(t, expected, string(r))
	})

	t.Run("regular_multiple_batch", func(t *testing.T) {
		var in interface{}
		data := []byte(`{
			"key1": "@{Bool.true}",
			"key2": "#{Arr.toto.tutu.tata}_#{Arr.1.2.3}"
		}`)
		err := json.Unmarshal(data, &in)
		require.NoError(t, err)
		tpl := Template{in}

		res, err := tpl.Resolve(resolver)
		require.NoError(t, err)
		expected := `[
			{"key1": true, "key2": "toto_1"},
			{"key1": true, "key2": "tutu_2"},
			{"key1": true, "key2": "tata_3"}
		]`
		r, err := json.Marshal(res)
		require.NoError(t, err)
		assert.JSONEq(t, expected, string(r))
	})

	t.Run("err_batch_noarray", func(t *testing.T) {
		var in interface{}
		data := []byte(`{
			"key1": "@{Bool.true}",
			"key2": "#{Num.3}"
		}`)
		err := json.Unmarshal(data, &in)
		require.NoError(t, err)
		tpl := Template{in}

		_, err = tpl.Resolve(resolver)
		require.Error(t, err)
	})

	t.Run("err_batch_diff_size", func(t *testing.T) {
		var in interface{}
		data := []byte(`{
			"key1": "@{Bool.true}",
			"key2": "#{Arr.toto.tutu.tata}",
			"key3": "#{Arr.foo.fee}"
		}`)
		err := json.Unmarshal(data, &in)
		require.NoError(t, err)
		tpl := Template{in}

		_, err = tpl.Resolve(resolver)
		require.Error(t, err)
	})
}

func TestResolveFromString(t *testing.T) {
	t.Run("fullstring_nobatch", func(t *testing.T) {
		res, err := resolveFromString("@{Num.2}", resolver, 0)
		require.NoError(t, err)
		assert.Equal(t, 2, res)
	})

	t.Run("fullstring_batch", func(t *testing.T) {
		res, err := resolveFromString("#{Arr.toto.tutu.tata}", resolver, 1)
		require.NoError(t, err)
		assert.Equal(t, "tutu", res)
	})

	t.Run("fullstring_batch_noarray_error", func(t *testing.T) {
		_, err := resolveFromString("#{Bool.false}", resolver, 1)
		require.Error(t, err)
	})

	t.Run("fullstring_resolve_error", func(t *testing.T) {
		_, err := resolveFromString("@{Err.error}", resolver, 1)
		require.Error(t, err)
	})

	t.Run("complexstr_2deps_nobatch", func(t *testing.T) {
		res, err := resolveFromString("prefix_@{Str.foo}_@{Num.5}", resolver, 1)
		require.NoError(t, err)
		assert.Equal(t, "prefix_foo_5", res)
	})

	t.Run("complexstr_2deps_batch", func(t *testing.T) {
		res, err := resolveFromString("prefix_@{Str.foo}_#{Arr.1.2.3.4.5}", resolver, 1)
		require.NoError(t, err)
		assert.Equal(t, "prefix_foo_2", res)
	})

	t.Run("complexstr_2deps_batch_noarray_error", func(t *testing.T) {
		_, err := resolveFromString("prefix_#{Str.foo}_@{Num.2}", resolver, 1)
		require.Error(t, err)
	})

	t.Run("complexstr_resolve_error", func(t *testing.T) {
		_, err := resolveFromString("prefix_#{Str.foo}_@{Err.error}", resolver, 1)
		require.Error(t, err)
	})

}
