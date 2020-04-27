package template

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

const (
	singlePrefix = "@"
	batchPrefix  = "#"
)

var (
	// tplRegexp is compiled regexp for templates in input structure
	tplRegexp *regexp.Regexp
)

func init() {
	r, err := regexp.Compile("([\\#|\\@|\\$]\\{[^}]+\\})")
	if err != nil {
		panic(errors.Wrap(err, "cannot compile template regexp"))
	}
	tplRegexp = r
}

// Template is a representation of the template.
type Template struct {
	input interface{}
}

// New returns a new Template from the given structure
func New(in interface{}) *Template {
	return &Template{
		input: in,
	}
}

// Expression is a a template element to be resolved
type Expression struct {
	Text    string
	IsBatch bool
}

func (expr Expression) String() string {
	prefix := singlePrefix
	if expr.IsBatch {
		prefix = batchPrefix
	}
	return fmt.Sprintf("%s{%s}", prefix, expr.Text)
}

// FindAll finds all expression within the given template
func (tpl *Template) FindAll() []Expression {
	var exprs []Expression
	find(&exprs, tpl.input)
	return exprs
}

func find(expressions *[]Expression, in interface{}) {
	if asMap, isMap := in.(map[string]interface{}); isMap {
		for _, v := range asMap {
			find(expressions, v)
		}
	}
	if asString, isString := in.(string); isString {
		exprs := findExpressions(asString)
		*expressions = append(*expressions, exprs...)
	}
	return
}

// findExpressions finds the template expression from the string
func findExpressions(in string) []Expression {
	var exprs []Expression
	strExprs := tplRegexp.FindAllString(in, -1)
	for _, str := range strExprs {
		e := asExpression(str)
		if e.Text != "" {
			exprs = append(exprs, e)
		}
	}
	return exprs
}

// asExpression create a tempate expression struct from a string.
func asExpression(in string) Expression {
	var str string
	batch := false
	if strings.HasPrefix(in, singlePrefix) {
		str = in[len(singlePrefix)+1 : len(in)-1]
	} else if strings.HasPrefix(in, batchPrefix) {
		str = in[len(batchPrefix)+1 : len(in)-1]
		batch = true
	} else {
		return Expression{}
	}
	if str != "" {
		return Expression{
			Text:    str,
			IsBatch: batch,
		}
	}
	return Expression{}
}
