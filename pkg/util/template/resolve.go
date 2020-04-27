package template

import (
	"fmt"
	"poseidon/pkg/util/maps"
	"reflect"

	"github.com/pkg/errors"
)

//ResolveFunc specifies how a template should be resolved
type ResolveFunc func(expr Expression) (interface{}, error)

// ResolveWithMap returns a ResolveFunc that performs resolution from a map
func ResolveWithMap(m map[string]interface{}) ResolveFunc {
	return func(expr Expression) (interface{}, error) {
		res := maps.Get(m, expr.Text)
		if res == nil {
			return nil, errors.Errorf("expression %s resolved to nil interface", expr)
		}
		return res, nil
	}
}

// Resolve resolves template using the given resolver
func (tpl *Template) Resolve(resolver ResolveFunc) (interface{}, error) {
	//First find expressions to determine batch size
	expressions := tpl.FindAll()
	if len(expressions) == 0 {
		// No input dependencies
		return tpl.input, nil
	}

	batchSize := 1
	hasBatch := false
	for _, e := range expressions {
		if e.IsBatch {
			val, err := resolver(e)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot resolve template expression %s", e)
			}
			v := reflect.ValueOf(val)
			if v.Kind() != reflect.Array && v.Kind() != reflect.Slice {
				return nil, errors.Errorf("template expression %s is flagged as batch but is not an array", e)
			}

			if hasBatch { // A batch expression has already been detected, check they have the same size
				if batchSize != v.Len() {
					return nil, errors.Errorf("error with template expression %s, at least an other batch expression has been detected with a different length", e)
				}
			} else {
				hasBatch = true
				batchSize = v.Len()
			}
		}
	}

	if hasBatch {
		results := make([]interface{}, batchSize)
		for i := 0; i < batchSize; i++ {
			r, err := resolve(tpl.input, resolver, i)
			if err != nil {
				return nil, err
			}
			results[i] = r
		}
		return results, nil
	}
	return resolve(tpl.input, resolver, 0)
}

func resolve(input interface{}, resolver ResolveFunc, index int) (interface{}, error) {
	val := reflect.ValueOf(input)
	switch val.Kind() {
	case reflect.String:
		return resolveFromString(input.(string), resolver, index)
	case reflect.Map:
		// Map is map[string]interface{}
		m := make(map[string]interface{})
		for k, v := range input.(map[string]interface{}) {
			newVal, err := resolve(v, resolver, index)
			if err != nil {
				return nil, err
			}
			m[k] = newVal
		}
		return m, nil
	case reflect.Array, reflect.Slice:
		var a []interface{}
		for i := 0; i < val.Len(); i++ {
			newVal, err := resolve(val.Index(i).Interface(), resolver, index)
			if err != nil {
				return nil, err
			}
			a = append(a, newVal)
		}
		return a, nil
	}
	return input, nil
}

func resolveFromString(input string, resolver ResolveFunc, index int) (interface{}, error) {
	expressions := findExpressions(input)
	if len(expressions) == 1 && len(input) == len(expressions[0].String()) {
		//If the input string is only a template expresion, it can be resolved as any type
		e := expressions[0]
		val, err := resolver(e)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot resolve template expression %s", e)
		}
		if e.IsBatch {
			v := reflect.ValueOf(val)
			if v.Kind() != reflect.Array && v.Kind() != reflect.Slice {
				return nil, errors.Errorf("template expression %s is flagged as batch but is not an array", e)
			}
			return v.Index(index).Interface(), nil

		}
		return val, nil
	}
	// Input string requires complex replacing, using regexp.ReplaceAllStringFunc
	var rerr error
	return tplRegexp.ReplaceAllStringFunc(input, func(matched string) string {
		e := asExpression(matched)
		val, err := resolver(e)
		if err != nil {
			rerr = errors.Wrapf(err, "cannot resolve template expression %s", e)
			return ""
		}
		var res interface{}
		if e.IsBatch {
			v := reflect.ValueOf(val)
			if v.Kind() != reflect.Array && v.Kind() != reflect.Slice {
				rerr = errors.Errorf("template expression %s is flagged as batch but is not an array", e)
				return ""
			}
			res = v.Index(index).Interface()
		} else {
			res = val
		}
		return fmt.Sprintf("%v", res)
	}), rerr
}
