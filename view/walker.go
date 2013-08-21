package view

import (
	"github.com/couchbaselabs/tuqtng/ast"
	"github.com/couchbaselabs/indexing/api"
	"bytes"
	"fmt"
)

type JsStatement struct {
	js bytes.Buffer
}

func NewWalker() *JsStatement {
	var js JsStatement
	return &js
}

func (this *JsStatement) JS() string {
	return this.js.String()
}

// inorder traversal of the AST to get JS expression out of it
func (this *JsStatement) Visit(e ast.Expression) (ast.Expression, error) {
	switch expr := e.(type) {
		case *ast.DotMemberOperator:
			if this.js.Len() == 0 {
				this.js.WriteString("doc.")
			}
			_, err := expr.Left.Accept(this)
			if err != nil {
				return nil, err
			}
			this.js.WriteString(".")
			_, err = expr.Right.Accept(this)
			if err != nil {
				return nil, err
			}
			
		case *ast.BracketMemberOperator:
			if this.js.Len() == 0 {
				this.js.WriteString("doc.")
			}
			_, err := expr.Left.Accept(this)
			if err != nil {
				return nil, err
			}
			this.js.WriteString("[")
			_, err = expr.Right.Accept(this)
			if err != nil {
				return nil, err
			}
			this.js.WriteString("]")
			
		case *ast.Property:
			if this.js.Len() == 0 {
				this.js.WriteString("doc.")
			}
			this.js.WriteString(expr.Path)
			
		case *ast.LiteralNumber:
			this.js.WriteString(fmt.Sprintf("%v", expr.Val)) 
		
		case *ast.LiteralString:
			this.js.WriteString(expr.Val)
		
		default:
			panic(expr)
			return e, api.ExprNotSupported
		
	}
	return e, nil
}
