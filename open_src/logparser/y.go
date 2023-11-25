// Code generated by goyacc sql.y. DO NOT EDIT.

//line sql.y:2
/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package logparser

import __yyfmt__ "fmt"

//line sql.y:18

import (
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

func setParseTree(yylex interface{}, stmts influxql.Statements) {
	for _, stmt := range stmts {
		yylex.(*YyParser).Query.Statements = append(yylex.(*YyParser).Query.Statements, stmt)
	}
}

//line sql.y:32
type yySymType struct {
	yys      int
	stmt     influxql.Statement
	stmts    influxql.Statements
	str      string
	query    influxql.Query
	int      int
	int64    int64
	float64  float64
	expr     influxql.Expr
	exprs    []influxql.Expr
	unnest   *influxql.Unnest
	strSlice []string
}

const EXTRACT = 57346
const AS = 57347
const LPAREN = 57348
const RPAREN = 57349
const IDENT = 57350
const STRING = 57351
const OR = 57352
const AND = 57353
const BITWISE_OR = 57354
const COLON = 57355
const COMMA = 57356

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"EXTRACT",
	"AS",
	"LPAREN",
	"RPAREN",
	"IDENT",
	"STRING",
	"OR",
	"AND",
	"BITWISE_OR",
	"COLON",
	"COMMA",
}

var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyInitialStackSize = 16

//line yacctab:1
var yyExca = [...]int8{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyPrivate = 57344

const yyLast = 47

var yyAct = [...]int8{
	14, 34, 8, 4, 37, 13, 29, 24, 17, 20,
	19, 19, 7, 21, 10, 33, 15, 16, 22, 23,
	36, 25, 27, 28, 26, 30, 20, 19, 10, 18,
	15, 16, 15, 16, 35, 31, 32, 5, 35, 38,
	12, 6, 11, 9, 2, 1, 3,
}

var yyPact = [...]int16{
	8, -1000, -1000, -1000, -4, -1000, -1000, 23, 16, -1000,
	22, -1000, 24, 24, -6, -1000, -1000, 8, 24, 22,
	22, -1, -1000, -1000, 24, -1000, 28, -1000, 0, -1000,
	-1000, 31, 9, 24, 13, -10, -1000, 24, -1000,
}

var yyPgo = [...]int8{
	0, 46, 3, 45, 44, 2, 43, 42, 0, 5,
	41, 40, 1, 37,
}

var yyR1 = [...]int8{
	0, 3, 4, 1, 2, 2, 2, 10, 5, 5,
	5, 5, 6, 6, 7, 11, 11, 13, 9, 9,
	12, 12, 8, 8,
}

var yyR2 = [...]int8{
	0, 1, 1, 1, 3, 1, 1, 1, 1, 3,
	3, 3, 1, 1, 1, 2, 2, 8, 1, 3,
	1, 3, 1, 1,
}

var yyChk = [...]int16{
	-1000, -3, -4, -1, -2, -13, -10, 4, -5, -6,
	6, -7, -11, -9, -8, 8, 9, 12, 6, 11,
	10, -5, -9, -9, 13, -2, -9, -5, -5, 7,
	-8, 7, 5, 6, -12, -8, 7, 14, -12,
}

var yyDef = [...]int8{
	0, -2, 1, 2, 3, 5, 6, 0, 7, 8,
	0, 12, 13, 14, 18, 22, 23, 0, 0, 0,
	0, 0, 16, 15, 0, 4, 0, 10, 11, 9,
	19, 0, 0, 0, 0, 20, 17, 0, 21,
}

var yyTok1 = [...]int8{
	1,
}

var yyTok2 = [...]int8{
	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14,
}

var yyTok3 = [...]int8{
	0,
}

var yyErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{}

//line yaccpar:1

/*	parser for yacc output	*/

var (
	yyDebug        = 0
	yyErrorVerbose = true
)

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

type yyParser interface {
	Parse(yyLexer) int
	Lookahead() int
}

type yyParserImpl struct {
	lval  yySymType
	stack [yyInitialStackSize]yySymType
	char  int
}

func (p *yyParserImpl) Lookahead() int {
	return p.char
}

func yyNewParser() yyParser {
	return &yyParserImpl{}
}

const yyFlag = -1000

func yyTokname(c int) string {
	if c >= 1 && c-1 < len(yyToknames) {
		if yyToknames[c-1] != "" {
			return yyToknames[c-1]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func yyErrorMessage(state, lookAhead int) string {
	const TOKSTART = 4

	if !yyErrorVerbose {
		return "syntax error"
	}

	for _, e := range yyErrorMessages {
		if e.state == state && e.token == lookAhead {
			return "syntax error: " + e.msg
		}
	}

	res := "syntax error: unexpected " + yyTokname(lookAhead)

	// To match Bison, suggest at most four expected tokens.
	expected := make([]int, 0, 4)

	// Look for shiftable tokens.
	base := int(yyPact[state])
	for tok := TOKSTART; tok-1 < len(yyToknames); tok++ {
		if n := base + tok; n >= 0 && n < yyLast && int(yyChk[int(yyAct[n])]) == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if yyDef[state] == -2 {
		i := 0
		for yyExca[i] != -1 || int(yyExca[i+1]) != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; yyExca[i] >= 0; i += 2 {
			tok := int(yyExca[i])
			if tok < TOKSTART || yyExca[i+1] == 0 {
				continue
			}
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}

		// If the default action is to accept or reduce, give up.
		if yyExca[i+1] != 0 {
			return res
		}
	}

	for i, tok := range expected {
		if i == 0 {
			res += ", expecting "
		} else {
			res += " or "
		}
		res += yyTokname(tok)
	}
	return res
}

func yylex1(lex yyLexer, lval *yySymType) (char, token int) {
	token = 0
	char = lex.Lex(lval)
	if char <= 0 {
		token = int(yyTok1[0])
		goto out
	}
	if char < len(yyTok1) {
		token = int(yyTok1[char])
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			token = int(yyTok2[char-yyPrivate])
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		token = int(yyTok3[i+0])
		if token == char {
			token = int(yyTok3[i+1])
			goto out
		}
	}

out:
	if token == 0 {
		token = int(yyTok2[1]) /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(token), uint(char))
	}
	return char, token
}

func yyParse(yylex yyLexer) int {
	return yyNewParser().Parse(yylex)
}

func (yyrcvr *yyParserImpl) Parse(yylex yyLexer) int {
	var yyn int
	var yyVAL yySymType
	var yyDollar []yySymType
	_ = yyDollar // silence set and not used
	yyS := yyrcvr.stack[:]

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yyrcvr.char = -1
	yytoken := -1 // yyrcvr.char translated into internal numbering
	defer func() {
		// Make sure we report no lookahead when not parsing.
		yystate = -1
		yyrcvr.char = -1
		yytoken = -1
	}()
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yytoken), yyStatname(yystate))
	}

	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	yyn = int(yyPact[yystate])
	if yyn <= yyFlag {
		goto yydefault /* simple state */
	}
	if yyrcvr.char < 0 {
		yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
	}
	yyn += yytoken
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = int(yyAct[yyn])
	if int(yyChk[yyn]) == yytoken { /* valid shift */
		yyrcvr.char = -1
		yytoken = -1
		yyVAL = yyrcvr.lval
		yystate = yyn
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	}

yydefault:
	/* default state action */
	yyn = int(yyDef[yystate])
	if yyn == -2 {
		if yyrcvr.char < 0 {
			yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && int(yyExca[xi+1]) == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = int(yyExca[xi+0])
			if yyn < 0 || yyn == yytoken {
				break
			}
		}
		yyn = int(yyExca[xi+1])
		if yyn < 0 {
			goto ret0
		}
	}
	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			yylex.Error(yyErrorMessage(yystate, yytoken))
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yytoken))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				yyn = int(yyPact[yyS[yyp].yys]) + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = int(yyAct[yyn]) /* simulate a shift of "error" */
					if int(yyChk[yystate]) == yyErrCode {
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yytoken))
			}
			if yytoken == yyEofCode {
				goto ret1
			}
			yyrcvr.char = -1
			yytoken = -1
			goto yynewstate /* try again in the same state */
		}
	}

	/* reduction by production yyn */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
	}

	yynt := yyn
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= int(yyR2[yyn])
	// yyp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = int(yyR1[yyn])
	yyg := int(yyPgo[yyn])
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = int(yyAct[yyg])
	} else {
		yystate = int(yyAct[yyj])
		if int(yyChk[yystate]) != -yyn {
			yystate = int(yyAct[yyg])
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 1:
		yyDollar = yyS[yypt-1 : yypt+1]
//line sql.y:64
		{
			setParseTree(yylex, yyDollar[1].stmts)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
//line sql.y:70
		{
			yyVAL.stmts = []influxql.Statement{yyDollar[1].stmt}
		}
	case 3:
		yyDollar = yyS[yypt-1 : yypt+1]
//line sql.y:76
		{
			yyVAL.stmt = yyDollar[1].stmt
		}
	case 4:
		yyDollar = yyS[yypt-3 : yypt+1]
//line sql.y:82
		{
			cond1, ok := yyDollar[1].stmt.(*influxql.LogPipeStatement)
			if !ok {
				yylex.Error("expexted LogPipeStatement")
			}
			cond2, ok := yyDollar[3].stmt.(*influxql.LogPipeStatement)
			if !ok {
				yylex.Error("expexted LogPipeStatement")
			}

			var unnest *influxql.Unnest
			if cond1.Unnest != nil && cond2.Unnest != nil {
				yylex.Error("only one extract statement is supported")
			} else {
				if cond1.Unnest != nil {
					unnest = cond1.Unnest
				} else {
					unnest = cond2.Unnest
				}
			}

			var cond influxql.Expr
			if cond1.Cond != nil && cond2.Cond != nil {
				cond = &influxql.BinaryExpr{Op: influxql.Token(influxql.AND), LHS: cond1.Cond, RHS: cond2.Cond}
			} else {
				if cond1.Cond != nil {
					cond = cond1.Cond
				} else {
					cond = cond2.Cond
				}
			}

			yyVAL.stmt = &influxql.LogPipeStatement{
				Cond:   cond,
				Unnest: unnest}
		}
	case 5:
		yyDollar = yyS[yypt-1 : yypt+1]
//line sql.y:119
		{
			yyVAL.stmt = &influxql.LogPipeStatement{Unnest: yyDollar[1].unnest}
		}
	case 6:
		yyDollar = yyS[yypt-1 : yypt+1]
//line sql.y:123
		{
			yyVAL.stmt = &influxql.LogPipeStatement{Cond: yyDollar[1].expr}
		}
	case 7:
		yyDollar = yyS[yypt-1 : yypt+1]
//line sql.y:129
		{
			yyVAL.expr = yyDollar[1].expr
		}
	case 8:
		yyDollar = yyS[yypt-1 : yypt+1]
//line sql.y:135
		{
			yyVAL.expr = yyDollar[1].expr
		}
	case 9:
		yyDollar = yyS[yypt-3 : yypt+1]
//line sql.y:139
		{
			yyVAL.expr = &influxql.ParenExpr{Expr: yyDollar[2].expr}
		}
	case 10:
		yyDollar = yyS[yypt-3 : yypt+1]
//line sql.y:143
		{
			yyVAL.expr = &influxql.BinaryExpr{Op: influxql.Token(influxql.AND), LHS: yyDollar[1].expr, RHS: yyDollar[3].expr}
		}
	case 11:
		yyDollar = yyS[yypt-3 : yypt+1]
//line sql.y:147
		{
			yyVAL.expr = &influxql.BinaryExpr{Op: influxql.Token(influxql.OR), LHS: yyDollar[1].expr, RHS: yyDollar[3].expr}
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
//line sql.y:153
		{
			yyVAL.expr = yyDollar[1].expr
		}
	case 13:
		yyDollar = yyS[yypt-1 : yypt+1]
//line sql.y:157
		{
			yyVAL.expr = yyDollar[1].expr
		}
	case 14:
		yyDollar = yyS[yypt-1 : yypt+1]
//line sql.y:163
		{
			yyVAL.expr = yyDollar[1].expr
		}
	case 15:
		yyDollar = yyS[yypt-2 : yypt+1]
//line sql.y:169
		{
			yyVAL.expr = &influxql.BinaryExpr{Op: influxql.Token(influxql.AND), LHS: yyDollar[1].expr, RHS: yyDollar[2].expr}
		}
	case 16:
		yyDollar = yyS[yypt-2 : yypt+1]
//line sql.y:173
		{
			yyVAL.expr = &influxql.BinaryExpr{Op: influxql.Token(influxql.AND), LHS: yyDollar[1].expr, RHS: yyDollar[2].expr}
		}
	case 17:
		yyDollar = yyS[yypt-8 : yypt+1]
//line sql.y:179
		{
			unnest := &influxql.Unnest{}
			unnest.Mst = &influxql.Measurement{}
			unnest.CastFunc = "cast"

			columnsemi, ok := yyDollar[3].expr.(*influxql.BinaryExpr)
			if !ok {
				yylex.Error("expexted BinaryExpr")
			}
			unnest.ParseFunc = &influxql.Call{
				Name: "match_all",
				Args: []influxql.Expr{
					&influxql.VarRef{Val: columnsemi.RHS.(*influxql.StringLiteral).Val},
					columnsemi.LHS},
			}

			unnest.DstColumns = []string{}
			dstFunc := &influxql.Call{
				Name: "array",
				Args: []influxql.Expr{},
			}
			for _, f := range yyDollar[7].strSlice {
				unnest.DstColumns = append(unnest.DstColumns, f)
				dstFunc.Args = append(dstFunc.Args, &influxql.VarRef{Val: "varchar"})
			}
			unnest.DstFunc = dstFunc

			yyVAL.unnest = unnest
		}
	case 18:
		yyDollar = yyS[yypt-1 : yypt+1]
//line sql.y:211
		{
			var expr influxql.Expr
			switch yyDollar[1].expr.(type) {
			case *influxql.Wildcard:
				expr = &influxql.BinaryExpr{Op: influxql.Token(influxql.NEQ), LHS: &influxql.VarRef{Val: "content", Type: influxql.String}, RHS: &influxql.StringLiteral{Val: ""}}
			case *influxql.VarRef:
				expr = &influxql.BinaryExpr{Op: influxql.Token(influxql.MATCHPHRASE), LHS: &influxql.VarRef{Val: "content", Type: influxql.String}, RHS: &influxql.StringLiteral{Val: yyDollar[1].expr.(*influxql.VarRef).Val}}
			default:
				expr = &influxql.BinaryExpr{Op: influxql.Token(influxql.MATCHPHRASE), LHS: &influxql.VarRef{Val: "content", Type: influxql.String}, RHS: yyDollar[1].expr}
			}
			yyVAL.expr = expr
		}
	case 19:
		yyDollar = yyS[yypt-3 : yypt+1]
//line sql.y:225
		{
			var field influxql.Expr
			if strVal, ok := yyDollar[1].expr.(*influxql.StringLiteral); ok {
				field = &influxql.VarRef{Val: strVal.Val}
			} else {
				field = yyDollar[1].expr
			}

			var expr influxql.Expr
			switch yyDollar[3].expr.(type) {
			case *influxql.Wildcard:
				expr = &influxql.BinaryExpr{Op: influxql.Token(influxql.NEQ), LHS: field, RHS: &influxql.StringLiteral{Val: ""}}
			case *influxql.VarRef:
				expr = &influxql.BinaryExpr{Op: influxql.Token(influxql.MATCHPHRASE), LHS: field, RHS: &influxql.StringLiteral{Val: yyDollar[3].expr.(*influxql.VarRef).Val}}
			default:
				expr = &influxql.BinaryExpr{Op: influxql.Token(influxql.MATCHPHRASE), LHS: field, RHS: yyDollar[3].expr}
			}
			yyVAL.expr = expr
		}
	case 20:
		yyDollar = yyS[yypt-1 : yypt+1]
//line sql.y:247
		{
			if _, ok := yyDollar[1].expr.(*influxql.VarRef); ok {
				yyVAL.strSlice = []string{yyDollar[1].expr.(*influxql.VarRef).Val}
			} else {
				yyVAL.strSlice = []string{yyDollar[1].expr.(*influxql.StringLiteral).Val}
			}
		}
	case 21:
		yyDollar = yyS[yypt-3 : yypt+1]
//line sql.y:255
		{
			if _, ok := yyDollar[1].expr.(*influxql.VarRef); ok {
				yyVAL.strSlice = append([]string{yyDollar[1].expr.(*influxql.VarRef).Val}, yyDollar[3].strSlice...)
			} else {
				yyVAL.strSlice = append([]string{yyDollar[1].expr.(*influxql.StringLiteral).Val}, yyDollar[3].strSlice...)
			}
		}
	case 22:
		yyDollar = yyS[yypt-1 : yypt+1]
//line sql.y:265
		{
			if yyDollar[1].str == "*" {
				yyVAL.expr = &influxql.Wildcard{Type: influxql.MUL}
			} else {
				yyVAL.expr = &influxql.VarRef{Val: yyDollar[1].str, Type: influxql.String}
			}
		}
	case 23:
		yyDollar = yyS[yypt-1 : yypt+1]
//line sql.y:273
		{
			yyVAL.expr = &influxql.StringLiteral{Val: yyDollar[1].str}
		}
	}
	goto yystack /* stack new state and value */
}
