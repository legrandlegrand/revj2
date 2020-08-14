"""
Reverse Snowflake Joins
Copyright (c) 2008, Alexandru Toth
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY Alexandru Toth ''AS IS'' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL <copyright holder> BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE."""


import re, sys, tempfile
from pyparsing import *
from constants import *

"""  r"''" used to be there, but it removes empty string constants"""
ESCAPEDQUOTES = ['""', r'\"', r"\'"]

""" long constants are truncated"""
MAX_CONST_LEN = 20

QUOTE = '?'

"""replace x='a' with x='_____0' """
QUOTESYMBOL = '_' * 5

""" expressions ex: "x in (1,2,3)" are replaced with 
"x IN_EQUAL '_____s00'" for easier parsing """
IN_EQUAL = 'in_equal'
NOT_IN_EQUAL = 'not_in_equal'

""" expressions ex: "x between 1 and 10" are replaced with
"x BETWEEN_EQUAL '_____s00'" for easier parsing """
BETWEEN_EQUAL = 'between_equal'
NOT_BETWEEN_EQUAL = 'not_between_equal'

NOT_LIKE = 'not_like'

""" add 1000 when replacing ex: count(DISTINCT) ; used in Simplifier"""
AGG_DISTINCT = 1000

""" for ORDER BT x DESC add a suffix to self.orders """
ORDER_DESC_SUFFIX = '_' * 10

"""prefix for tables and parentTables in subselects"""
SUBSELECT = '___SUBSELECT___'

"""code is organized classes with process()
Suggested procesing order:
SanityChecker -> QuoteRemover -> Simplifier

SQL is difficult to parse. It is easier to build small functions that 
do one thing well and "consume" the processed SQL.

It is also possible to write many of the functions as pure Regex-es.
However PyParsing is more convenient"""


class MultipleSelectsException(Exception):
	"""expecting no subselects"""
	pass

class MallformedSQLException(Exception):
	"""obviously bad SQL"""
	pass
	
class REVJProcessingException(Exception):
	"""revj bugs"""
	pass


class NaturalJoinException(Exception):
	"""select * from table1 natural join table 2
	no info on columns"""
	pass
	
def keywordFromList(l):
	return eval(
		" | ".join(['Keyword("%s", caseless=True)' % x
			for x in l]))

def keywordParensFromList(l):
	return eval(
		" | ".join(['(Keyword("%s", caseless=True) + "(" + ")")' % x
			for x in l]) )

"""grammar is hard to read if it is polluted by self.*
Another alternative is to have these at top level ... """
def initGlobalGrammar():		
	#MySQL speciffic, add others later
	globals()["aggregatesAsList"] = ('avg bit_and bit_or bit_xor count '
		'group_concat '
		'first last max min std '
		'stddev_pop stddev_samp stddev stddevp '
		'sum var varp var_pop var_samp variance').split(' ')
	globals()["aggregatesRe"] = "_[0-9]+_agg"
	globals()["aggregates"] = Regex(aggregatesRe)
	
	globals()["reserved"] = \
		('select from where group having order and or not null exists is as ' +
		'in asc desc ' + 
		'by on inner outer left right full cross join using ' +
		'case when then end ' + 
		'distinct limit ' +
		'separator').split(' ') + aggregatesAsList
	
	#no params + not even parens 
	globals()["funcsNoParensAsList"] = ["current_date", "sysdate", "rownum"]	
	#no params with parens
	globals()["funcsNoParamsAsList"] = funcsNoParensAsList + \
		["curdate", "pi", "random"]
	globals()["funcsNoParams"] = keywordParensFromList(funcsNoParamsAsList)
		
	globals()["arithSign"] = Word("+-",exact=1)
	globals()["intNum"] = Regex("[\+\-]?\d+")
	globals()["realNum"] = Regex("[\+\-]?(\d+\.\d*|\.\d+)")
	""" This optimization proposal by Paul McGuire doesn't work yet
	globals()["eNum"] = Regex("[\+\-]?(\d+\.\d*|\.\d+)[Ee][\+\-]?\d+")"""
	globals()["eNum"] = Combine(
		Optional(arithSign) +
		(	Combine(Word(nums) + Optional("." + Word(nums))) |
			Combine("." + Word(nums)) ) +
		(Literal("E") | Literal("e"))  +
		Optional(arithSign) +
		Word(nums) )

	globals()["dot"] = Literal(".")
	globals()["comma"] = Literal(",")
	
	"""	|| doesn't work with oneOf """
	binop_ = "+ - * / % ^ & | ~"
	globals()["binopAsList"] = binop_.split()
	globals()["binop"] = oneOf(binop_)   # ? div, mod?


	globals()["_not"] = Keyword('not', caseless=True)
	"""in_equal handles IN clause: "x in (1,2,3)" """
	globals()["in_equal"] = Keyword(IN_EQUAL, caseless=True)
	globals()["not_in_equal"] = Keyword(NOT_IN_EQUAL, caseless=True)
	"""between equal handles BETWEEN "x between 1 and 10" """
	globals()["between_equal"] = Keyword(BETWEEN_EQUAL, caseless = True)
	globals()["not_between_equal"] = Keyword(NOT_BETWEEN_EQUAL, caseless = True)
	globals()["_like"] = Keyword('like', caseless=True)
	globals()["not_like"] = Keyword(NOT_LIKE, caseless=True)
	globals()["compar"] = oneOf(
			"!= !> !< <> <= >= ^= /= += -= %= &= |= #= =# #=# = < >") | \
		not_in_equal | in_equal | not_between_equal | between_equal | \
		_like | not_like

	globals()["select"] = Keyword("select", caseless=True)
	globals()["sqlKeywordAsList"] = ["select", "where", "by", "having"]
	globals()["sqlKeyword"] = keywordFromList(sqlKeywordAsList)

	globals()["_and"] = Keyword("and", caseless=True)
	globals()["_or"] = Keyword("or", caseless=True)
	globals()["_from"] = Keyword('from', caseless=True)
	globals()["_inner"] = Keyword('inner', caseless=True)
	globals()["_outer"] = Keyword('outer', caseless=True)
	globals()["_left"] = Keyword('left', caseless=True)
	globals()["_right"] = Keyword('right', caseless=True)
	globals()["_full"] = Keyword('full', caseless=True)
	globals()["_join"] = Keyword('join', caseless=True)
	globals()["_on"] = Keyword('on', caseless=True)
	globals()["_in"] = Keyword('in', caseless=True)
	globals()["_between"] = Keyword('between', caseless=True)
	globals()["_where"] = Keyword('where', caseless=True)
	globals()["_group"] = Keyword('group', caseless=True)
	globals()["_order"] = Keyword('order', caseless=True)
	globals()["_eq"] = Literal('=')
	globals()["_semicolon"] = Literal(';')

	globals()["_allJoins"] = _inner | _outer | _left | _right | \
		_full | _join

	globals()["preNoBinop"] = sqlKeyword | "(" | ","  | compar
	globals()["pre"] = preNoBinop | binop | _and | _or
	globals()["preFiltersJoins"] =  _on | _and | _or | "(" | ","
	globals()["postJoins"] =  _and | _or | _allJoins | ")" | _where | \
		_group | _order | _semicolon
		
	#same with regex-es only
	globals()["reWS"] = r"\s*"
	globals()["reBetweenParens"] = r"(?P<inner>[^()]+)"
	globals()["rePre"]  = r"(?P<pre>[(+=<>#,]|and|or|select|where|by|having)\s*"
	globals()["rePost"] = r"(?P<post>[)+=<>#,]|and|or|from|where|group|order|having)\s*"
	globals()["ReColumnNameNoStar"] = \
		r"(?P<col>[0-9_$@]*[a-zA-Z][a-zA-Z0-9_$@\.]*)"

	reConst = r"('_____[0-9]+'"
	for c in funcsNoParamsAsList:
		reConst += "|" + c + r"\s*\(\s*\)"		
	globals()["reConst"] = reConst + ')'
	globals()["reCompar"] = r"[+=<>#]+"
	

	#*? = non-greedy matching
	globals()["quotedConst"] = Regex("'.*?'")

	#* is an operator and a column name .. this confuses the Simplifier
	globals()["identNoStarChars"] = "a-zA-Z0-9_$@"
	globals()["identNoStar"] = Regex("[0-9_$@]*[a-zA-Z][a-zA-Z0-9_$@]*")
	globals()["bind"] = Regex(":[a-zA-Z0-9_$]*")
	globals()["columnNameNoStar"] = delimitedList(identNoStar, dot, combine=True)
	globals()["ident"] = Literal('*') | identNoStar
	globals()["columnName"] = delimitedList(ident, dot, combine=True)

	#includes funcs without params ex: curdate()
	globals()["filterConst"] = intNum | quotedConst | \
		Group(ident + Literal("(") + Literal(")") ) | \
		bind
		
	globals()["inConstruct"] = "(" + reCompar + "|" + r"\s" + '[iI][nN]' + reWS + ")"
	globals()["reInSubselect"] = ReColumnNameNoStar + reWS + inConstruct + \
			reWS + r"\[" + reWS + r"(?P<nr>[0-9]+)" + \
			reWS + r"\]"
			
	globals()["reSubselectAlias"] = r"\[" + reWS + r"(?P<nr>[0-9]+)" + \
			reWS + r"\]" + reWS + ReColumnNameNoStar
			
	globals()["reSubselectNoAlias"] = r"\[" + reWS + r"(?P<nr>[0-9]+)" + \
			reWS + r"\]"
			
		
class BadParensException(Exception):
	""" this is reporting confusing error because processed SQL is
	different than original"""
	pass

class BadIdentException(Exception):
	""" this is reporting confusing error because processed SQL is
	different than original"""
	pass
	
class AmbiguousColumnException(Exception):
	"""in case there are more tables it is needed to use field.alias
	in SQL it is not important, but revj has no idea about fields from tables"""
	pass
	

"""count parens and quotes"""
class SanityChecker:
	#paranteze dupa removeConst
	#ghilimele dupa removeQuoteEscapes ?
	def checkParensHelper(self, s, openingP, closingP):
		cnt = 0
		for p in range(len(s)):
			c = s[p]
			if c == openingP:
				cnt += 1
			elif c == closingP:
				cnt -= 1
			if cnt < 0:
				#you can show where the extra ')' is 
				raise BadParensException('Too many closed \\%s in [%s]'%
					(closingP, s[:p]))
		if cnt != 0:
			raise BadParensException('Too many opened \\%s in [%s]' % 
				(openingP, s))
		return True
		
	def quoteCounter(self, s, q):
		return sum([1 for x in s if x == q])
		
	def checkParens(self, s):			
		if self.quoteCounter(s, '"') % 2 != 0:
			raise BadParensException('Unbalanced double quotes ')
			
		#next quote checks not working ???		
		if self.quoteCounter(s, "'") % 2 != 0:
			raise BadParensException('Unbalanced quotes' )
			
		res = self.checkParensHelper(s, '(', ')')
		res = res and self.checkParensHelper(s, '{', '}')
		res = res and self.checkParensHelper(s, '[', ']')
 
		return res

"""inside quotes there can be extra parens, sql keywords, functions,
escaped quotes. Escaped quotes are lost, but constants and E-numbers are
stored during parsing and displayed

e-nums don't technically belong here, but removing them early simplifies
the grammar for SQL

real nums contain ".", also used as table.field separator. Remove them

Return a lowercase string"""
class QuoteRemover(SanityChecker):
	def __init__(self):
		self.quotedConsts = {}

	"""call this before each new SQL parsing!"""
	def reset(self):
		self.quotedConsts = {}
		
	def removeComments(self, s):
		multilineCmt = re.compile('/\*.*?\*/' , re.DOTALL)
		res = multilineCmt.sub(' ', s)

		res = res.strip() + "\n"  #easier search for final end of line comment
		#remove end of line comments ex: --bla bla
		res = re.sub("\-\-.*\\n", ' ', res)
		res = res.strip()		
		return res
			
	"""remove stuff that is SQL dialect speciffic 
		and irrelevant to the diagram"""
	def removeUnknown(self, s):
		res = s
		res = self.removeMySQLdialect(res)
		res = self.removeMSdialect(res)
		res = self.removePGdialect(res)
		res = self.removeCaseWhen(res)
		res = self.removeSelectDistinct(res)
		return res
	
	def removeMySQLdialect(self, s):
		return re.sub('\sSEPARATOR\s', ',', s, re.IGNORECASE)
			
	def removePGdialect(self, s):
		pgTypes = 'bigint bigserial bit boolean bytea char character date ' + \
			'enum double int4 int8 integer numeric oid ' + \
			'serial text time timestamp varchar xml'
			
		res = s
		#drop the type
		for k in pgTypes.split(' '):
			res = res.replace('::'+k, '::')
			
		#drop the optional length specifier
		res = re.sub('::\s*\(\s*\d*\s*\)', '::', res)
		
		#drop the '::'
		res =  res.replace('::', '')		
		
		#drop the slicing operator x[2] or x[2:4]
 		res = re.sub(r"\[[0-9]+(:[0-9]+)?\]", '', res)
		
		return res
				
	def removeMSdialect(self, s):
		res = s
		p = re.compile('\s*with\(\s*nolock\s*\)',  re.IGNORECASE)
		res = p.sub(' ', res) 
		return res
		
	def removeCast(self, s):
		castTypes = 'bigint bigserial bit boolean bytea char character date ' + \
			'enum double int4 int8 integer numeric oid ' + \
			'serial text time timestamp varchar xml'
			
		res = s
		
		for k in castTypes.split(' '):
			res = re.sub('as\s+%s' % k, '', res)
			
		return res
		
	"""select distinct is not relevant to the join diagram. surprise.. """
	def removeSelectDistinct(self, s):
		res = s
		p = re.compile('select\s*distinct',  re.IGNORECASE)
		res = p.sub('select ', res) 
		return res
		
	def removeCaseWhen(self, s):
		rePat = re.compile(r"case\s+when", re.IGNORECASE)
		res = re.sub(rePat, ' ', s)
		
		for c in "when then else".split():
			rePat = re.compile(
				r'(?P<pre>[ (])%s(?P<post>[ )])' 
				% c, re.IGNORECASE)
				
			res = re.sub(rePat, '\g<pre> , \g<post>', res, re.IGNORECASE)
			
		for c in "case end".split():
			rePat = re.compile(
				r'(?P<pre>[ (])%s(?P<post>[ )])' 
				% c, re.IGNORECASE)
				
			res = re.sub(rePat, '\g<pre>  \g<post>', res, re.IGNORECASE)
		
		return res
		

	""" could have been a function;
		but it is confusing to have funcs and methods called *Quote*

		Remove all escaped or double quotes
		no sense in having them as schema/table/column names"""
	def removeQuoteEscapes(self, s):
		#irreversible changes!
		res = s
		for q in ESCAPEDQUOTES:
			res = res.replace(q, QUOTE)
		return res

	def dictInsertHelper(self, long_x):
		if len(long_x) > MAX_CONST_LEN:
			x = long_x[:MAX_CONST_LEN] + '...'
		else:
			x = long_x
		d = self.quotedConsts
		if x in d:
			#function is called over and over. Constant is replaced in SQL 
			#if done == it is having integer key
			assert int(str(d[x])) == d[x]
			return " '%s%d' " % (QUOTESYMBOL, d[x])
		else:
			l = len(d)
			d[x] = l
			return " '%s%d' " % (QUOTESYMBOL, l)

	""""replace all string constants and e-nums with _____s0 .. _____s999"""
	def removeConst(self, s):
		myConst = pre.setResultsName("pre") + \
			( quotedConst | eNum | realNum | intNum ).setResultsName("inner")
		myConst.setParseAction(lambda x: x.pre + " " + \
			self.dictInsertHelper(x.inner))

		res = myConst.transformString(s)
		return res

	def removeInClause(self, s):
		nic = _not + _in + Literal("(") + \
			delimitedList( filterConst, comma).setResultsName('lst') + \
			Literal(")")

		nic.setParseAction(lambda x: ' ' + NOT_IN_EQUAL + ' ' + \
			self.dictInsertHelper('not in (' + ','.join(x.lst) + ")" ) )

		res = nic.transformString(s)

		#easier to repeat than to make Optional(_not)
		ic = _in + Literal("(") + \
			delimitedList( filterConst, comma).setResultsName('lst') + \
			Literal(")")

		ic.setParseAction(lambda x: ' ' + IN_EQUAL + ' ' + \
			self.dictInsertHelper('in (' + ','.join(x.lst) + ")" ) )

		res = ic.transformString(res)

		return res

	def removeBetween(self, s):
		nbc = _not + _between + filterConst.setResultsName('c0') + _and + \
			filterConst.setResultsName('c1')

		nbc.setParseAction(lambda x: ' ' + NOT_BETWEEN_EQUAL + ' ' + \
			self.dictInsertHelper('not between ' + x.c0 + ' and ' + x.c1))

		res = nbc.transformString(s)
		#easier to repeat than to make Optional(_not)
		bc = _between + filterConst.setResultsName('c0') + _and + \
			filterConst.setResultsName('c1')

		bc.setParseAction(lambda x: ' ' + BETWEEN_EQUAL + ' ' + \
			self.dictInsertHelper('between ' + x.c0 + ' and ' + x.c1))

		res = bc.transformString(res)
		return res
		
	def removeNOTLike(self, s):
		notLike = re.compile('not\slike' , re.DOTALL + re.IGNORECASE)
		return notLike.sub(NOT_LIKE, s)

	def removeSquareBrackets(self, s):
		sb = Literal("[") + columnName.setResultsName('inner') + Literal("]")
		sb.setParseAction(lambda x: '"' + x.inner + '"' )
		res = sb.transformString(s)
				
		return res
		
	def removeTrueFalse(self, s):
		rePat = "(?P<pre>[ *+-/=#<>&|(){}%])" +  r"true" + \
			"(?P<post>[ *+-/=#<>&|(){}%;])"
		res = re.sub(rePat, '\g<pre> 1 \g<post>', s, re.IGNORECASE)
		
		rePat = "(?P<pre>[ *+-/=#<>&|(){}%])" +  r"false" + \
			"(?P<post>[ *+-/=#<>&|(){}%;])"

		res = re.sub(rePat, '\g<pre> 0 \g<post>', res, re.IGNORECASE)

		return res
		
	#append extra parens to funcs without params and without parens
	def replaceFuncsNoParens(self, s):
		res = s
		for f in funcsNoParensAsList:
			rePat = "(?!=[a-z0-9_$@])(%s)(?!=[a-z0-9_$@])" % f
			res = re.sub(rePat, '\g<1>()', res, re.IGNORECASE)
			
		return res
			
		
	def removeCurlyBraces(self, s):
		sb = Literal("{") + columnName.setResultsName('inner') + Literal("}")
		sb.setParseAction(lambda x: x.inner)
		res = sb.transformString(s)
				
		return res

	def restoreConst(self, s):
		replacedConst = (Literal("'").suppress() +
			Literal(QUOTESYMBOL).suppress() +
			Word(nums) +
			Literal("'").suppress())
		replacedConst.setParseAction \
			(lambda x: "'" + self.quotedConsts[x] + "'")
		res = replacedConst.transformString(s)
		return res

	""" "table name"."column name" -> table$name.column$name
	double definition of characters not allowed in quotedIdent
	due to usage of SkipTo() -> better error reporting?"""
	def removeQuotedIdent(self, s):
		quotedIdent = (Literal('"').suppress() +
			SkipTo(Literal('"')).setResultsName("inner") +
			Literal('"').suppress())

		bads = list("')(+-*/%#[]{}&!@=;:")
		for e in quotedIdent.searchString(s):
			for b in bads:
				if b in e[0]:
					raise BadIdentException('Bad quotes/ident [%s] [%s]'
					% (e, s) )

		quotedIdent.setParseAction(lambda x: x.inner.replace(' ', '$'*5))
		res = quotedIdent.transformString(s)
		
		#this includes "" and \"
		if '"' in res:
			raise BadParensException('Unbalanced double quotes in [%s]' % s)
		
		return res

	def removeUTF(self, s):
		"""mySQL speciffic; other dialects have similar quoting"""
		res = s.replace("_utf8'", "'")

		"""Postgres speciffic syntax"""
		return res.replace("::text", " ")

	def getQuotedConstsDict(self):
		res = {}
		for (k,v) in self.quotedConsts.items():
			res[QUOTESYMBOL + str(v)] = k

		return res

	def process(self, s):
		self.reset()
		res = self.removeComments(s)

		#do this only after removing end-of-line comments !!!
		#for multi-line SQL
		res = re.sub("[\n\r\t]", ' ', res)
		
		#high priority MySQL specific backquoting for column aliases
		res = res.replace('`', '"')
		
		#remove cast
		res = self.removeCast(res)

		#MS SQL specific quoting for aliases 
		res = self.removeSquareBrackets(res)
		
		#dollar substitution. Remove braces, keep dollar
		res = self.removeCurlyBraces(res)
		
		res = self.removeUTF(res)
		res = self.removeQuoteEscapes(res)
		res = self.removeTrueFalse(res)
		
		#IN clause
		res = self.removeInClause(res)
		
		
		#BETWEEN clause
		res = self.removeBetween(res)
		
		res = self.removeNOTLike(res)
		
		#replace 'systime' with 'systime()' == that already works 
		res = self.replaceFuncsNoParens(res)

		#remove stuff irrelevant to diagrams
		res = self.removeUnknown(res)
		
		res = self.removeConst(res)
		
		res = self.removeQuotedIdent(res)

		#there can be parens inside quotes
		self.checkParens(res)
		return res.lower()


"""over-simplification of SQL
reduces all operands to +
reduces func(a,b..) to a+b+..
reduces all constant operations

Result contains only schemas, tables, columns, constants, comparisons
comparisons are where clauses OR joins
"""
class Simplifier:
	def __init__(self):
		self.reset()

	def reduceBinops(self, s):
		""" || is not oneOf() because is more chars"""
		res = s.replace('||', '+')
			
		before = identNoStarChars + "\'\)"
		after = identNoStarChars + "\'\("
		letterOpLetter = "([%s])[ ]*[%s][ ]*([%s])" % \
			(before, "\\".join(binopAsList), after)
		res = re.sub(letterOpLetter, r"\1 + \2", res, re.DOTALL)
			
		return res
		
	
	def distinctAggregsHelper(self, x):
		aggToReplace = aggregatesAsList.index(x.agg)
		if x.dist <> '':
			aggToReplace += AGG_DISTINCT 
		res = x.pre + " _%d_agg(" % aggToReplace
		return res		
	
	""" there are a lot of aggregation funcs. This slows parsing 
	replace sum(x) with 0__agg(x) """
	def replaceAggregs(self, s):
		origAggregates = keywordFromList(aggregatesAsList)
		agExpr = (preFiltersJoins | select | _from).setResultsName('pre') + \
			origAggregates.setResultsName('agg') + Literal("(") + \
			Optional(Keyword('distinct', caseless=True)).setResultsName('dist')
			
		agExpr.setParseAction(lambda x: self.distinctAggregsHelper(x) )
		
		res = agExpr.transformString(s)
		return res
		
	""" (+) looks as an expression in parens and is reduced"""
	def reduceOuterJoin(self, s):
		loj = (Literal("(") + "+" + ")" + "=") | \
			Literal("+") + Literal("=")
		loj.setParseAction(lambda: '#=')

		roj = Literal("=") + "(" + "+" + ")" | \
			Literal("=") + Literal("+")
		roj.setParseAction(lambda: '=#')

		oj = loj | roj
		return oj.transformString(s)
		
	"""chop sql into small pieces"""
	def smallChunksGen(all):
		crtPos = 0
		parensNest = 0
		for (pos, ch) in enumerate(all):
			if ch == '(':
				parensNest += 1
			elif ch == ')':
				parensNest -= 1
				if parensNest == 0:
					yield all[crtPos:pos+1]
					crtPos = pos+1
		yield all[crtPos:]
		
	def runRemoversOnChunks(self, removers, s):
		res = []
		for piece in smallChunksGen(s):
			res.append(self.runRemovers(removers, piece))
			
		res = ' '.join(res)
		return res
		#once more on the whole thing ?
		#return self.runRemovers(res)
			
		
	def runRemovers(self, removers, s):
		x = s
		change = True
		while change:
			change = False
			for r in removers:
				"""some grammar is space sensitive / easier to write Regex
				less CPU intensive with re than pyparsing"""
				x = re.sub("\s+", ' ', x)
				x = re.sub("\s*([\+\,])\s*", r"\1", x)
				
				x = self.runRegexRemoverConstantOps(x)
				x = self.runRegexRemoverParensInExpressions(x)
				x = self.runRegexRemoverConstEqualConst(x)

				newX = r.transformString(x)
				change = change or (newX != x)
				x = newX
					
		x = x.replace('select+from', 'select * from')

		assert s.islower()
		#test against dropping space after "pre"
		assert x.startswith('select ')			

		return x
		
	def runRegexRemoverConstantOps(self, s):
		"""remove constant expressions"""
		
		#commas are needed ex for '..., 2008 as year, ..' 				
		# + const -> ' '
		rePat = r"\+" + reWS + reConst
		res = re.sub(rePat, ' ', s)
		
		# , const ,  -> 
		#why this is not handled by previous ??
		#rePat2 = r"[,]"  + reWS + reConst + r"[,]"
		#res = re.sub(rePat2, ' + ', res)
		
		# pre const +  -> pre
		rePat3 = rePre  + reConst + reWS + r"[+,]"
		res = re.sub(rePat3, '\g<pre> ', res)
		
		# , const ) -> )
		rePat4 = r"[+,]" + reWS + reConst + reWS + r"[)]"
		res = re.sub(rePat4, ')', res)
		
		return res

	def runRegexRemoverParensInExpressions(self, s):
		"""remove parens in expressions
		CharsNotIn("()=<>") is needed to process later '*' = '*' """

		rePat = rePre + reWS + r"\(" + r"(?P<inner>[^()=<>]*?)" + r"\)"
		res= re.sub(rePat, '\g<pre> \g<inner>', s)
		return res
		
	def runRegexRemoverConstEqualConst(self, s):
		"""remove "ALL" style filters generated by some reporting tools
		Parens are needed to protect against matching: a + 'x' = 'y' + b """
		
		# ('*'='*' OR ..
		rePat = r"\(" + reWS + reConst + reWS + reCompar + \
			reWS + reConst + reWS + r"(and|or)"
		res = re.sub(rePat, '(', s)
		# .. OR '*' = '*')
		rePat1 = r"(and|or)" + reWS + reConst + reWS + \
			r"[+-*/=<>#]+" + reWS + reConst + r"\s*\)"
			
		res = re.sub(rePat, ')', res)
		return res
	
	def buildRemovers(self):
		constantVal = quotedConst | funcsNoParams
		
		plus_ = Literal('+')

		expression = Combine(
			columnName +
			ZeroOrMore(plus_ + (columnName | constantVal) ))

		self.removers = []

		"""remove functions, parens and commas"""
		aggFunc = aggregates + \
			Literal('(') + \
			columnName + \
			Literal(')')			
		aggTerm = aggFunc | constantVal | columnName
		binopComma = plus_ | comma
		aggExpression = delimitedList(aggTerm, binopComma, combine=True)
		func = pre.setResultsName("pre") + \
			~(aggregates) + ~(funcsNoParams) + columnName + \
			Literal('(') + \
			aggExpression.setResultsName('inner') + \
			Literal(')')
		#Regex(r"[a-z0-9]+[^()]*(\([^()]*\))*[^()]*").setResultsName('inner') + \
		func.setParseAction(lambda x: x.pre + " " + x.inner.replace(',', '+'))
		self.removers.append(func)	


	""" replace space in 'Group by' and 'Order by' with '_' """
	def reduceOrderGroup(self, s):
		_by = Keyword('by', caseless = True)
		ob = Keyword('order', caseless = True) + _by
		ob.setParseAction(lambda : 'order_by')
		gb = Keyword('group', caseless = True) + _by
		gb.setParseAction(lambda : 'group_by')

		x = ob.transformString(s)
		x = gb.transformString(x)
		return x

	def reset(self):
		self.removers = []

	def process(self, s):
		x = self.reduceBinops(s)
		x = self.replaceAggregs(x)
		x = self.reduceOuterJoin(x)

		self.reset()
		self.buildRemovers()

		res = self.runRemovers(self.removers, x)
		
		return self.reduceOrderGroup(res)


def checkIdentifier(x):
	return ( (x.lower() not in reserved) and 
		(re.search(aggregatesRe, x) is None) and 
		not (x.startswith("'" + QUOTESYMBOL)) )
	
def addAliasIfOK(d, k, v):
	if checkIdentifier(k) and checkIdentifier(v):
		addAlias(d, k, v)

"""insert into d[k] = Set(.. v ..) """
def addAlias(d, k, v):	
	if k in d:
		if v not in ['', None]:
			d[k].add(v)
	else:
		if v in ['', None]:
			d[k] = set()
		else:
			temp = set()
			temp.add(v)
			d[k] = temp
		
"""get table from table.field or schema.table.field"""
def getFirstTwoDots(v):
	temp = v.split(".")
	if len(temp) == 1: # column
		return ''
	if len(temp) == 2: #table.column
		return temp[0]

	if len(temp) == 3:	#schema.table.column
		return temp[0] + '.' + temp[1]

	raise MallformedSQLException('Too may dots in identifier: %s' % s)

"""get table or column name from schema.table.column"""
def getLastDot(v):
	temp = v.split(".")
	if len(temp) <= 3:	#schema.table.column
		return temp[-1]

	raise MallformedSQLException('Too may dots in identifier: %s' % s)
	
def splitByCommasWithoutParens(s):
	res = []
	parenCnt = 0
	oldPos = 0
	for pos, c in enumerate(s):
		if c == '(':
			parenCnt += 1
		elif c == ')':
			parenCnt -= 1
			assert parenCnt >= 0
		elif c == ',' and parenCnt == 0:
			res.append( s[oldPos+1:pos].strip() )
			oldPos = pos
	
	if s[oldPos] == ',':
		oldPos += 1
		
	res.append(s[oldPos:].strip() )
	
	return res
	
def checkNotExpr(y):
	return [z for z in y if z in "()+=<>' *"] == []

"""Basic Select .. From .. Where.. Group by .. Order by .. ;
-no subselects, no EXISTS, no UNION
"""
class SingleSelect:
	""" QuoteRemover.quotedConsts will contain replaced consts
		after each round of process()"""
	def __init__(self, qr = None):
		self.reset()

		self.qr = qr

	def reset(self, parentTables2 = {}):
		"""tableAliases has all tables . colAliases only aliases == inconsistent !!
		"""
		self.tableAliases = {}	#schema.table -> Set of aliases
		self.parentTables = parentTables2	
		self.derivedTables = {} #alias -> index of subselect in the SQL Stack
		self.subselects = {} #alias -> subselect
		self.unions = [] # [ [union/union_all/.. , index of subselect]]		
		
		self.colAliases = {}	#tblAlias.field -> Set of aliases
		self.projectionCols = set()	#alias or col, may be without table. prefix
		self.exprAliases = set()	#tblAlias.colAlias 
		self.filters = {}		#tblAlias.field -> set( "= 1", "> 10"..)

		#the outer join side is uppercase!
		self.joins = {}			#tblAlias0.field0 -> set(tblAlias.field1)

		self.groups = set()		#tblAlias.field
		self.orders = set()		#tblAlias.field
		self.aggregs = {}		#tblAlias.field -> set(min, max..)
		self.havings = {}		#tblAlias.field -> set("sum(x) > 100"..)

		#table.field. Collects all values from other dicts
		self.columns = set()
		
		"""true if there is "select * .."
		Used only to display * column in all tables"""
		self.selectStar = False

	def sanityCheckColumns(self):		
		for r in reserved:
			if r in self.columns:
				raise REVJProcessingException("[%s] is a column name!" % r)
				
		for c in self.columns:
			if c.startswith("'" + QUOTESYMBOL):
				raise REVJProcessingException("[&s] is a constant, not column!" 
					% c)
				
	def checkAmbiguousColumns(self):
		if len(self.tableAliases) == 1 and len(self.parentTables) == 0:
			return
			
		for r in self.columns:
			if (r not in self.exprAliases) and \
				(r not in self.colAliases.values()) and \
				(r not in self.orders) and \
				(r + ORDER_DESC_SUFFIX not in self.orders):
				#aliases & orders are columns too, but have no table prefix !!
				if len(r.split('.')) < 2 and r <> '*':
					raise AmbiguousColumnException(
						"use table.field instead of %s!" % r)


	"""subprocess* methods are called from process* methods"""
	
	
	"""find all column names between select..from	
	this is a method that processes everything that looks as column name .. """
	def subprocessSelectColumns(self, s):
		asAlias = Suppress(Keyword('as') + ident)
		noAliases = asAlias.transformString(s)

		for c in columnName.searchString(noAliases):
			#single * is a valid ident, Ex count(*)
			if checkIdentifier(c[0]) and (('*' not in c[0]) or ('.*' in c[0]) ):
				#function names are not columns
				pat = c[0] + reWS + '\('
				if not re.search(pat, s):
					self.columns.add(c[0])

			if c[0] == '*':
				self.selectStar = True

	"""helper for aggregs lambda func"""
	def aggregLambda(self, x):
		#get index out of '_15_agg'
		aggPat = r'_(?P<nr>\d*)_agg'
		m = re.search(aggPat, x.agg)
		aggIdx = int( m.group('nr') )

	
		#add optional DISTINCT for '_1015_agg'
		if aggIdx >= AGG_DISTINCT:
			distinct = 'DISTINCT '
			aggIdx -= AGG_DISTINCT
		else:
			distinct = ''
		
		"""need to use alias.field into self.aggregs and self.havings
		later on, DotOutput will drop the "alias." """
		
		if x.comp <> '' and x.const <> '':
			newComp = self.reverseComparisonSign(x)
			addAliasIfOK(self.havings, x.inner, 
				aggregatesAsList[aggIdx].upper() + 
				"(" + distinct + x.inner + ")" + newComp + " " + 
				self.replaceConstsWithOrig(x.const))
		else:
			addAliasIfOK(self.aggregs, x.inner, 
				aggregatesAsList[aggIdx].upper() + 
				"(" + distinct + x.inner + ")")

	"""SELECT .. FROM ; HAVING .. may contain aggregates
	for HAVING get whole expression 'sum(x)=100' """
	def subprocessAggregs(self, s):
		s = ',' + s	#easier grammar

		ag = aggregates
		agExpr = preFiltersJoins.suppress() + \
			(	filterConst.setResultsName("const") +
				compar.setResultsName("comp") +
				ag.setResultsName('agg') +
				"(" +
				columnName.setResultsName('inner') +
				")" ) | \
			(	ag.setResultsName('agg') +
				"(" +
				columnName.setResultsName('inner') +
				")" +
				Optional(compar.setResultsName("comp") +
					filterConst.setResultsName("const") ) )
		
		agExpr.setParseAction( lambda x: self.aggregLambda(x) )
		res = agExpr.transformString(s)
		
	"""sum(t.a * 100) belongs in table t"""
	def findTableOfExpression(self, s):
		cols = []
		#negative lookahead to exclude "function("
		pat = ReColumnNameNoStar + reWS + r'(?!\()'
		for m in re.finditer(pat, s):
			cols.append(m.group('col'))
			
		tbls = set([getFirstTwoDots(x) for x in cols])
		try:
			tbls.remove('')
		except KeyError:
			pass
		
		if len(tbls) == 1:
			return tbls.pop() + '.'
		return ''		
	
	""" find projected columns, some may have no table. prefix
	find column as alias; one column may have multiple aliases
	for complex expressions get only alias name
	"""
	def processColAliases(self, s):
		s = ' ' + s
		withoutAliases = []
		for part in splitByCommasWithoutParens(s):
			try:
				(exprPart, aliasPart) = part.strip().rsplit(' ', 1)
				
				if ')' in aliasPart:
					#for example count (distinct x)
					raise ValueError	#fall thru no alias
					
					
				exprPart = exprPart.strip()
				if exprPart.endswith(' as'):
					exprPart = exprPart[:-3]
					
				withoutAliases.append(exprPart)				
				
				if checkIdentifier(aliasPart):
					tbl = self.findTableOfExpression(exprPart)						
					self.projectionCols.add(tbl + aliasPart)
										
					if checkNotExpr(exprPart):
						addAliasIfOK(self.colAliases, exprPart, aliasPart)					
					else:
						if tbl == '':
							self.exprAliases.add(aliasPart)
						else:
							#this is an alias to an expression that depends
							#only on columns from one table
							addAliasIfOK(self.colAliases, tbl + '_', aliasPart)
			except ValueError:
				#split fails == no alias
				if checkNotExpr(part) and checkIdentifier(part):
					self.projectionCols.add(part)
				withoutAliases.append(part)
			

		x = ',' + ','.join(withoutAliases)	#easier grammar
		self.subprocessSelectColumns(x)
		self.subprocessAggregs(',' + s)

	"""restore constant previously processed by QuoteRemover
	BETWEEN does not need extra enclosing "'" """
	def replaceConstsWithOrig(self, c, quote = "'"):
		if isinstance(c, list):
			# functions with no params are constants too
			# ['pi', '(', ')' ]			
			return c
			
		#c='"' + QUOTESYMBOL + number + '"'
		try:
			if not c[1:].startswith(QUOTESYMBOL):
				return c
		except:		
			return c
			
		try:
			dummy = int(c[1 + len(QUOTESYMBOL):-1])
		except ValueError, TypeError:
			return c

		try:
			return self.revQuotedConsts[c[1:-1]]
		except KeyError:
			raise REVJProcessingException(
				'Missing previously replaced constant %s !' % c)

		return c

	def negateSign(self, s):
		if s == '<':
			return '>'
		elif s == '>':
			return '<'
		elif s == '<=':
			return '>='
		elif s == '>=':
			return '<='

		return s #other signs not processed ex '<>'

	"""reverse comparison sign. Ex: '0<x' means 'x>0' """
	def reverseComparisonSign(self, searchRes):
		newComp = self.negateSign(searchRes.comp)
		if searchRes.const == searchRes[0]:
			return newComp

		return searchRes.comp


	""" table.col = CONST or table.col IS NULL"""
	def subprocessFilters(self, s):
		s = ',' + s	#easier grammar

		colCompConst = preFiltersJoins.suppress() + \
			(( 	columnName.setResultsName("col") +
				compar.setResultsName("comp") +
				filterConst.setResultsName("const") ) | \
			( 	filterConst.setResultsName("const") +
				compar.setResultsName("comp") +
				columnName.setResultsName("col") ) | \
			(	columnName.setResultsName("col") +
				Literal("is").setResultsName("comp") +
				Literal("null").setResultsName("const") ) | \
			(	columnName.setResultsName("col") +
				Literal("is").setResultsName("comp") +
				(Literal("not") + Literal("null")).setResultsName("const") ) )

		for ccc in colCompConst.searchString(s):
			if ccc.col != 'select':
				newComp = self.reverseComparisonSign(ccc)
				newComp = newComp.replace('#', '')
				if ccc.comp in [IN_EQUAL, NOT_IN_EQUAL, BETWEEN_EQUAL, 
						NOT_BETWEEN_EQUAL]:
					"""x in (1,2,3) needs to be replaced twice:
					once for '(1,2,3) and once for in_equals '_____s02'"""
					newConst = self.replaceConstsWithOrig(
						self.replaceConstsWithOrig(ccc.const), '')
				else:
					newConst = self.replaceConstsWithOrig(ccc.const)
					if repr(newConst) == repr( (['not', 'null'], {}) ):
						newConst = 'not null'
					if newComp == NOT_LIKE:
						newComp = 'not like'
				try:
					addAliasIfOK(self.filters, ccc.col, newComp +' '+ newConst)
				except:
					#handle one param funcs, that come as [ ident, "(", ")" ]					
					addAliasIfOK(self.filters, ccc.col, 
						newComp + ' ' + ''.join(ccc.const) )
						
	"""handling table.field in [ 1 ] , meaning a subselect"""
	def subprocessInSubselect(self, s):
		for m in re.finditer(reInSubselect, s):
			self.columns.add(m.group('col'))

	"""handling alias for subselects [ 1 ] as alias"""
	def subprocessSubselectAlias(self, s):
		for m in re.finditer(reSubselectAlias, s):
			self.subselects[m.group('col')] = m.group('nr')
			
		for m in re.finditer(reSubselectNoAlias, s):
			if m.group('nr') not in self.subselects.values():
				tbl = 'anon_subselect_' + m.group('nr')
				self.subselects[tbl] = m.group('nr')
				addAliasIfOK(self.tableAliases, tbl, tbl)
				
	"""handling UNION INTERSECT .."""
	def subprocessUnions(self, s):
		u = (	(Keyword('union', caseless=True) | 
				Keyword('intersect', caseless=True) | 
				Keyword('except', caseless=True) ) + \
			Optional(Keyword('all', caseless=True))).setResultsName('union') + \
			OneOrMore(Literal('[').suppress() + intNum.setResultsName('nr') + Literal(']').suppress()).setResultsName('nrs') 

		for i in u.scanString(s):
			start = i[1]
			for u in i[0].nrs:
				self.unions.append( [ i[0].union, u] )
			#there can be only one UNION at the very end
			return s[:start]
			
		return s

	"""handling the join condition ex t1.a=t2.b """
	def subprocessJoins(self, s):
		s = ',' + s	+ ';' #easier grammar
		outer = Literal("(") + Literal("+") + Literal(")")
		colCompCol = preFiltersJoins.suppress() + \
			Optional(outer).setResultsName('left') + \
			columnName.setResultsName("col0") + \
			compar.setResultsName("comp") + \
			columnName.setResultsName("col1") + \
			Optional(outer).setResultsName('right') + \
			FollowedBy(postJoins)

		for i in colCompCol.searchString(s):
			source = i.col0
			dest = i.col1
			if i.comp.strip()[0] == '#' or i.left != '':
				source = source.upper()
			if i.comp.strip()[-1] == '#' or i.right != '':
				dest = dest.upper()

			addAliasIfOK(self.joins, source, dest)
			addAliasIfOK(self.joins, dest, source)

	def addTableEmptyAlias(self, t, a):	
		avoidUnion = ['', 'union', 'except', 'intersect', 'all']
		if a is None or a in avoidUnion:
			if t not in avoidUnion:
				addAliasIfOK(self.tableAliases, t, t)
		else:
			addAliasIfOK(self.tableAliases, t, a)

	def subprocessAnsiJoins(self, s):
		s = s + ';'
		
		joinCondElement = ~_allJoins + (
			filterConst | columnName | compar | "(" | ")" | "(+)" | _and | _or)

		joinsAndConditions = ZeroOrMore(_allJoins).setResultsName('allJoins')+\
			Optional('(') + \
			columnName.setResultsName('t1') + \
			Optional(Keyword('as')) + \
			Optional(~(_on) + columnName.setResultsName('t1alias')) + \
			Optional(')') + \
			_on + OneOrMore(joinCondElement).setResultsName('joinConditions')+\
			FollowedBy(_allJoins | ";" | ',')

		for i in joinsAndConditions.searchString(s):
			self.addTableEmptyAlias(i.t1, i.t1alias)
			join = ' '.join(i.joinConditions)
			
			if ('full' in repr(i.allJoins)):
				join = join.replace('=', '#=#')
			elif ('left' in repr(i.allJoins)):
				join = join.replace('=', '#=')
			elif ('right' in repr(i.allJoins)):
				join = join.replace('=', '=#')

			self.subprocessJoins(join)
			#sometimes filters are mixed into join conditions
			self.subprocessFilters(join)
			
		return s
			
	""" t1 join t2 using (x,y,z) """ 
	def subprocessAnsiJoinsUsing(self, s):
		rePat = ReColumnNameNoStar.replace('<col>', '<t1>') + reWS + \
			r"(inner)?" + reWS + \
			r"(?P<dir>(left|right|full)?)" + reWS + \
			r"(outer)?" + reWS + \
			"join" + reWS + \
			ReColumnNameNoStar.replace('<col>', '<t2>') + reWS + \
			"using" + reWS + r"\(" + reWS + reBetweenParens + \
			reWS + r"\)"
			
		for m in re.finditer(rePat, s):
			self.addTableEmptyAlias(m.group('t1'), '')
			self.addTableEmptyAlias(m.group('t2'), '')
			
			eq = '='
			if 'left' in m.group('dir'):
				eq = '#='
			elif 'right' in m.group('dir'):
				eq = '=#'
			elif 'full' in m.group('dir'):
				eq = '#=#'
			
			joinText = []
			for jCol in m.group('inner').split(','):
				joinText.append('%s.%s %s %s.%s' % 
					(m.group('t1'), jCol, eq, m.group('t2'), jCol)) 
				
			self.subprocessJoins(' and '.join(joinText))
			
		res = re.sub(rePat, ' ', s)
		return res

	def subprocessCrossJoin(self, s):
		rePat = ReColumnNameNoStar.replace('<col>', '<t1>') + reWS + \
			r"cross " + reWS + "join" + reWS + \
			ReColumnNameNoStar.replace('<col>', '<t2>') 
			
		for m in re.finditer(rePat, s):
			self.addTableEmptyAlias(m.group('t1'), '')
			self.addTableEmptyAlias(m.group('t2'), '')
		

	def subprocessTablesAliases(self, s):
		fromPart = ZeroOrMore(Literal('(')) + \
				columnName.setResultsName('t0') + \
				Optional(Keyword('as')) + \
				Optional(~(_allJoins) + columnName.setResultsName('t0alias'))
				
		for i in fromPart.searchString(s):
			if i.t0 not in reserved and \
					(i.t0alias == '' or i.t0alias not in reserved):
				self.addTableEmptyAlias(i.t0, i.t0alias)

	""" select x.a .. from (select ..) as x """
	def subprocessDerivedTables(self, s):
		rePat = reWS + r"\[" + reWS + r"(?P<nr>[0-9]+)" + reWS + r"\]" + reWS + \
			"as" + reWS + ReColumnNameNoStar.replace('<col>', '<alias>')
		for m in re.finditer(rePat, s):
			if checkIdentifier(m.group('alias')):
				self.derivedTables[m.group('alias')] = int(m.group('nr'))
				addAliasIfOK(self.tableAliases, 
					m.group('alias'), m.group('alias'))	
		
		#cut optonal as: ... [ 1 ] as alias ....
		rePat1 = r"\]" + reWS + "[aA][sS]" + reWS
		res = re.sub(rePat1, '] ', s)
		
		#cut subselect nr :  .. [ 1 ] alias ..
		rePat2 = r"\[" + reWS + r"(?P<nr>[0-9]+)" + reWS + r"\]" 
		
		res = re.sub(rePat2, '', res)
		
		return res
	
	def processTables(self, s):
		nj = Keyword('natural', caseless=True) + \
			Keyword('join', caseless=True)
		try:
			dummy = nj.searchString(s)[0]
		except IndexError:
			pass
		else:
			#if [0] succeds, there is bad 'natural join'
			raise NaturalJoinException(
				'Natural Join does not contain explicit column names, [%s]'
				% s)
	
		s = self.subprocessDerivedTables(s)
		s = self.subprocessAnsiJoinsUsing(s)
		self.subprocessMixedTablesAndAnsiJoins(s)
		self.subprocessCrossJoin(s)
		return self.subprocessAnsiJoins(s)
		
	def subprocessMixedTablesAndAnsiJoins(self, s):
		#process separately beginning and ANSI joins for the rest
		sep = r"\s" + \
			"left|right|full|join|inner|outer".replace('|', r"\s|\s") + '|,'			
		sepPatern = re.compile(sep)
		tablesAliasesPart = sepPatern.split(s)
		for part in tablesAliasesPart:
			if ' on ' not in part:
				self.subprocessTablesAliases(part)		

	def processWhereGroupOrderHaving(self, reservedWord, s):
		if reservedWord == 'having':
			self.subprocessAggregs(s)
			
		#' '_'*10 appended at the end of the column to show ORDER DESC
		if reservedWord == 'order_by':
			if s[-1] == ';':
				s = s[:-1]
			s = ',' + s + ','
			s = s.replace(' desc,', ORDER_DESC_SUFFIX + ',')
					
		for c,start,end in columnName.scanString(s):
			if checkIdentifier(c[0]) and \
					(('*' not in c[0]) or ('.*' in c[0]) ) and \
					(len(compar.searchString(c[0])) == 0):			
				if reservedWord == 'group_by':
					self.groups.add(c[0])
				elif reservedWord == 'order_by':
					if c[0].endswith(ORDER_DESC_SUFFIX):
						#check what is before
						if s[:start].strip().endswith(','):
							#ex: ...,c desc, ...
							self.orders.add(c[0])
						else:
							#ex: ...a+b desc ...
							self.orders.add(c[0][:-len(ORDER_DESC_SUFFIX)])
					else:
						self.orders.add(c[0])


	"""table from table.field must match with schema.table"""
	def sanityCheckTables(self):
		aliasesFromColumns = set( [getFirstTwoDots(x) for x in self.columns] )
		#get table names
		tablesAndAliases = set( [getLastDot(x) for x in 
			self.tableAliases.keys() + self.parentTables.keys()] )

		#get schema from schema.table
		tablesAndAliases = tablesAndAliases.union(self.tableAliases.keys())
			
		for tas in self.tableAliases.values() + self.parentTables.values():
			#append from set of aliases
			tablesAndAliases = tablesAndAliases.union(
				set( [getLastDot(x) for x in tas] ) )
								
		for t in aliasesFromColumns:
			if t <> '' and t not in tablesAndAliases:
				raise MallformedSQLException(
					'identifier %s refers to unknown table or alias' % t)

	""" self.columns contains all joins, filters..
	Easier to write output"""
	def addColumnsFromOthers(self):		
		for s in 'aggregs groups filters havings colAliases exprAliases'.split():
			for j in eval('self.' + s):
				self.columns.add(j)
				
		#Extra ORDER_DESC_SUFFIX at end for ORDER DESC
		for j in self.orders:
			if j.endswith(ORDER_DESC_SUFFIX):
				self.columns.add(j[:-len(ORDER_DESC_SUFFIX)])
			else:
				self.columns.add(j)			

		for i in self.joins:
			for j in self.joins[i]:
				#outer joins are uppercase
				self.columns.add(i.lower())
				self.columns.add(j.lower())

	def addStarToAllTables(self):
		if self.selectStar:
			for table in self.tableAliases:
				for alias in self.tableAliases[table]:
					alias2 = alias
					if alias == '':
						alias2 = table
					self.columns.add(alias2 + ".*")
		
	""" renames a to t.a in:
	select a from table t; """
	def aliasSingleSelectCols(self):
		if len(self.tableAliases) != 1:
			return
			
		(tbl, alias) = self.tableAliases.items()[0]		
		
		try:
			alias = list(alias)[0] #tbl -> set of aliases, remember?
		except TypeError, IndexError:
			alias = tbl
			
		alias = alias + '.'
				
		#dict: aggregs filters colAliases havings
		#it could do keys() part of joins, but usually joins require 
		#_several_ tables. For now, the self joins have to be written by hand
		for s in 'aggregs filters colAliases havings'.split():
			for j in eval('self.' + s):
				if '.' not in j:	
					cmd = 'self.%s[alias + "%s"] = self.%s.pop("%s")' % \
						(s, j, s, j)
					#print cmd
					exec(cmd)

		#set: groups exprAliases columns
		for s in 'groups orders exprAliases columns'.split():
			if eval('len(self.%s)' % s) == 0:
				continue
				
			tmp = set()
			for j in eval('self.' + s):
				if s=='columns' and j.startswith(QUOTESYMBOL):
					#this is a constant placeholder, not a column
					continue
					
				if '.' not in j:
					tmp.add(alias + j)
				else:
					tmp.add(j)

			cmd = 'self.%s = tmp' % s
			#print cmd, tmp			
			exec(cmd)
			
		#exprAliases become columns
		for e in self.exprAliases:
			self.columns.add(e)
			
		self.exprAliases = set()
		
		tmp = set()
		for p in self.projectionCols:
			if p.startswith(alias):
				tmp.add(p)
			else:
				tmp.add(alias + p)
				
		self.projectionCols = tmp
				
	def process(self, s, parentTables = {}):
		self.reset(parentTables)
		
		#fix for postJoin
		if s[-1] != ';':
			s = s + ';'

		self.revQuotedConsts = self.qr.getQuotedConstsDict()
		
		s = self.subprocessUnions(s)

		sep = "select|from|where|group_by|order_by|having"
		sep2 = (sep + r"\s").replace('|', r"\s|\s")
				
		sepPatern = re.compile("(" + sep2 + ")")

		parts = sepPatern.split(s)
		
		"""MultipleSelectsException is different because
		it can be handled with subselect"""
		if len([i for i in parts if i.startswith('select')]) <> 1:
			raise MultipleSelectsException(
				'Only one [select] expected in [%s]' % s)
				
		if len(s.split(' using ')) > 2:
			raise AmbiguousColumnException(
			'max two tables can be joined relaibly with using clause')

		assert parts[0] == '' #missing select
		assert parts[1].startswith('select')
				
		for i in sep.split('|'):
			if len([x for x in parts if x == ' ' + i + ' ']) > 1:
				raise MallformedSQLException(
					'Only one [%s] expected in [%s]' % (i, s) )

		self.processColAliases(parts[2])
		for i in xrange(len(parts)-3):
			if parts[i + 3] == ' from ':
				self.processTables(parts[i + 4])
				self.subprocessSubselectAlias(parts[i + 4])
			elif parts[i + 3].strip() in 'order_by group_by having where'.split():
				self.processWhereGroupOrderHaving(parts[i + 3].strip(), parts[i + 4])
			#duplicate processing for WHERE !!
			if parts[i + 3] == ' where ':
				self.subprocessFilters(parts[i + 4])
				self.subprocessJoins(parts[i + 4])
				self.subprocessInSubselect(parts[i + 4])
				
		self.aliasSingleSelectCols()

		self.addColumnsFromOthers()
		
		self.sanityCheckColumns()		
		self.checkAmbiguousColumns()
		
		self.sanityCheckTables()
		self.addStarToAllTables()
				
		return self.tableAliases

"""adds cluster prefix only to tables in sub-selects"""
def formatCluster(clusterNr):
	if clusterNr == 0:
		return ''
	return '%s_%d_' % (SUBSELECT, clusterNr)


"""generates Graphviz diagrams in .dot format"""
class DotOutput:
	""" ss = instance of SimpleSelect. Updated after each .process()"""
	def __init__(self, ss):
		self.ss = ss

	def unquoteSpaceInAlias(self, s):
		if '$'*5 in s:
			return r'\"' + s.replace('$'*5, ' ') + r'\"'
		else:
			return s

	def dotUnQuote(self, v):
		res = v.replace("<", r"\<")
		res = res.replace(">", r"\>")
		res = res.replace("|", r"\|")
		res = res.replace('"', r'\"')
		res = res.replace(NOT_IN_EQUAL, " ")
		res = res.replace(IN_EQUAL, " ")
		res = res.replace(NOT_BETWEEN_EQUAL, " ")
		res = res.replace(BETWEEN_EQUAL, " ")
		return res
		
	""" table has one extra alias, formated as __SUBSELECT__2, meaning where in the SQL stack it comes from"""
	def extractParent(self, x):
		for t in self.ss.parentTables:
			if ((x == t) or #x is table
				(x in self.ss.parentTables[t])) : #x is alias			
				for i in self.ss.parentTables[t]:
					if SUBSELECT in i:
						return int(i[len(SUBSELECT):])
		
	def isInParent(self, x):			
		if (x in self.ss.tableAliases) or (x in reduce(set.union, self.ss.tableAliases.values())):
			return -1
			
		res = []
			
		if x in self.ss.parentTables:
			res.append( self.extractParent(x) )
		else:
			#check the aliases for tables
			for t in self.ss.parentTables:
				if x in self.ss.parentTables[t]:
					res.append( self.extractParent(t) )
					
		if len(res) > 1:
			print "gaga"
			raise Exception("duplicated parent tables/aliases")
		elif len(res) == 1:
			return res[0]
		else:
			return -1
		
	def getColor(self, c):
		if c == 0:
			return 'white'
		return CLUSTERCOLORS[c % len(CLUSTERCOLORS)]
		
	#remove funny chars from graphviz edge and node names
	#(if needed, use quoting in labels ..) 
	def dotSanitize(self, s):
		return s.replace('$','').replace('.','__')
		
	"""drop table name from count(t.x) and count(DISTINCT t.x)
	also used for HAVING clauses"""
	def DistinctFieldFormatter(self, fld):
		firstRParen = fld.find(')')
		
		lParenOrSpace = fld[:firstRParen].rfind(' ')
		if lParenOrSpace == -1:
			lParenOrSpace = fld.find('(') # no DISTINCT
			
		#handle also schema.table.field 
		prevDot = fld[:firstRParen].rfind('.')
		if prevDot == -1:
			prevDot = lParenOrSpace
			
		return fld[:lParenOrSpace+1] + fld[prevDot+1:]

	def formatField(self, c):
		res = []
		lc = getLastDot(c).lower()
		#if c in self.ss.joins or c.upper() in self.ss.joins:
		#optimize : somehow display one column only once
		
		if lc <> '_':
			#_ means expression alias referring to cols in this table
			res.append('<%s> ' % self.dotSanitize(lc) )
			
			if c in self.ss.filters:
				temp = lc + ' '
				for i in self.ss.filters[c]:
					temp += self.dotUnQuote(i) + ','
				res.append(temp[:-1]) #cut last ','

			if c in self.ss.groups:
				res.append('GROUP BY ' + lc)
				
			if c in self.ss.orders:
				res.append('ORDER BY ' + lc)
				
			if c + ORDER_DESC_SUFFIX in self.ss.orders:
				res.append('ORDER BY %s DESC' % lc)

			if c in self.ss.aggregs:
				for i in self.ss.aggregs[c]:
					res.append(self.DistinctFieldFormatter(i))

			if c in self.ss.havings:
				for i in self.ss.havings[c]:
					res.append('HAVING %s' %
						self.dotUnQuote(self.DistinctFieldFormatter(i)))

			#in case there are no joins, no filters ...
			if len(res) == 1:
				if (c in self.ss.colAliases) and (lc in self.ss.colAliases[c]):
					#this is an expression alias, will be displayed later
					res = []
				else:
					#just column name, no label
					res[0] = res[0] + lc	
			else:
				res[1] = res[0] + res[1]
				res = res[1:]
			
		try:
			for a in self.ss.colAliases[c]:
				if lc == '_':
					res.append(
						'<%s> ... AS %s' % (a, self.unquoteSpaceInAlias(a)))
				else:
					res.append(
						'<%s> %s AS %s' % (a, lc, self.unquoteSpaceInAlias(a)))
		except:
			pass
		return '|'.join(res)
		
	def formatTable(self, t, a, clusterNr = 0):		
		tbl = self.dotSanitize(a.upper())
		headerTbl = tbl
		clr = clusterNr
		
		if t in self.ss.derivedTables:
			headerTbl = 'DERIVED TABLE'
			clr = self.ss.derivedTables[t]
			
		res = ['\t%s%s [style=filled, fillcolor=%s, label="%s | (%s) ' % 
			(formatCluster(clusterNr), self.dotSanitize(a.upper()), self.getColor(clr), 
			headerTbl, t.upper()) ]

		sortedCols = []
		for c in self.ss.columns:
			if getFirstTwoDots(c) == a:
				sortedCols.append(c)

		for c in sorted(sortedCols):
			res.append('|' + self.formatField(c) )

		res.append('"];')
		return ''.join(res)

	def genNodes(self, clusterNr = 0):
		res = []
		for t in self.ss.tableAliases:
			if 1: #t not in self.ss.derivedTables:
				for a in self.ss.tableAliases[t]:
					res.append(self.formatTable(t, a, clusterNr))

		return res
		
	"""aliases for expressions are displayed in a separate table"""
	def genExprAliasNodes(self):
		res = []
		for a in enumerate(self.ss.exprAliases):
			res.append('\t_expr_%d [label="... AS %s"];' % a)
			
		return res			
		
	"""aliases do not have table name. This func avoids ":alias" """
	def getFirstLastDots(self, fld):
		lastDot = getLastDot(fld)
		firstTwoDots = self.dotSanitize(getFirstTwoDots(fld)) + ':'
		if firstTwoDots == ':':
			firstTwoDots = ''
			
		return (lastDot, firstTwoDots)
		

	"""gvpr needs both arrowhead and arrowtail in order to reverse the edge"""
	def formatJoin(self, i, j, iClusterNr, jClusterNr):
		#firstTwoDots === table_alias name OR '' for expression aliases 
		(lastDot_i, firstTwoDots_i) = self.getFirstLastDots(i)
		(lastDot_j, firstTwoDots_j) = self.getFirstLastDots(j)
		
		iCluster = formatCluster(iClusterNr)
		jCluster = formatCluster(jClusterNr)
		
		res = '\t' + iCluster + firstTwoDots_i.upper() + lastDot_i.lower() + \
			' -- ' + jCluster + firstTwoDots_j.upper() + lastDot_j.lower()

		outer = ''
		if i.isupper() :
			outer = 'arrowtail="%s"' % OUTERJOINARROW
		else:
			outer = 'arrowtail="none"'

		if j.isupper():
			outer = outer + ' arrowhead="%s"' % OUTERJOINARROW
		else:
			outer = outer + ' arrowhead="none"'

		if i.isupper() or j.isupper():
			color = OUTERJOINCOLOR
		else:
			color = 'black'

		res += ' [color = %s %s]' % (color, outer)

		return res + ';'

	def genEdges(self, clusterNr=0):
		"""returns two sets of edges: 
			joins inside the cluser = subselect AND
			joins from parent 
			
			AND joins between alias and subselects"""
		res = ([], [], [])
		for i in self.ss.joins:
			for j in self.ss.joins[i]:
				if i.lower() <= j.lower():	#avoid duplication; self joins are ok
					parentI = self.isInParent(getFirstTwoDots(i))
					parentJ = self.isInParent(getFirstTwoDots(j))
					if parentI >= 0:
						res[1].append(self.formatJoin(i, j, parentI, clusterNr))
					elif parentJ >= 0:
						res[1].append(self.formatJoin(i, j, clusterNr, parentJ))
					else:
						res[0].append(self.formatJoin(i, j, clusterNr, clusterNr))
						
		res[2].extend( self.genSubselectEdges(clusterNr))
		
		for (u, nr) in self.ss.unions:
			edge = '\n%s_dummy -> %s_dummy [color = black, arrowtail="none", arrowhead="none", label="%s"];\n' % \
				(formatCluster(clusterNr), formatCluster(int(nr)), ' '.join(u))
			

			res[2].append(edge)
			
		return res
		
	def genSubselectEdges(self, clusterNr):
		res = []
		for i in self.ss.subselects:
			res.append('%s%s -> %s_dummy [color = black, arrowtail="none", arrowhead="none"];' % (formatCluster(clusterNr), i.upper(), 
				formatCluster(int(self.ss.subselects[i]))))

		return res
		
	def checkFanChasmTraps(self):
		res = ''
		
		tablesWithAgg = set([getFirstTwoDots(a) for a in self.ss.aggregs])
		
		if len(tablesWithAgg) > 1:
			w = ','.join(self.ss.aggregs.keys())
			res = r'WARNING [label="Risk of Fan and/or Chasm trap: \l' + \
				r'There are aggregates in more than one table:\l %s"' % w + \
				r'fontcolor=red color=red];'
				
		return res 

	def process(self, clusterNr = 0):
		""" clusterNr == 0 means it's a main graph"""
		if clusterNr == 0:
			res = ['graph ', '{', '\tnode [shape=record, fontsize=12];',
				'\tgraph [splines=true];', 
				'\trankdir=LR;', 
				'\tdir=none;',
				'\t\t_dummy [shape=none, label=""];',
				'']
		else:
			res = ['\tsubgraph cluster_%d {' % clusterNr,
				'\t\tstyle=filled;',
				'\t\tcolor=%s;' % self.getColor(clusterNr) ,
				'\t\tnode [shape=record, fontsize=12];',
				'\t\tgraph [splines=true];', 
				'\t\trankdir=LR;', 
				'\t\tdir=none;',
				'\t\tlabel="subselect %d";' % clusterNr,
				'\t\t%s_dummy [shape=none, label=""];' % formatCluster(clusterNr),
				'']
		res.extend(self.genNodes(clusterNr) )
		res.extend(self.genExprAliasNodes() )
		res.append('')
		edges = self.genEdges(clusterNr)
		
		res.append(self.checkFanChasmTraps() )
		
		res.extend(edges[0] )	#joins inside subselect
		if clusterNr == 0:
			res.append('}')
		else:
			res.append('\t}')
			
		#(graph_as_dot, edges_coming_from_outer_select)
		return ('\n'.join(res) , edges[1], edges[2])

class SelectAndSubselects:
	def __init__(self):
		pass		
		
	def parenCount(self, s):
		return s.count('(') - s.count(')')
					
	def getSubselectLen(self, s):
		if s[0] == '(':
			# ex "(select ..) CUT HERE .."
			endCount = 0
		else:
			# ex "select ... CUT HERE) .."
			endCount = -1
			
		"""parens provide clear separation of the subquery"""
		parens = 0
		for i in xrange(len(s)):
			if s[i] == '(':
				parens += 1
			if s[i] == ')':
				parens -= 1
			if parens == endCount:
				return i + 1 + endCount
				
		"""if the select in not enclosed in parens, maybe is not subselect 
		
		UNION belongs to the next SELECT
		Or, it should not be reduced with the most nested SELECT"""
		"""if ' union' == s[-6:]:
			return len(s)-6"""
		return len(s)
		
	def getMostNested(self, s):
		separator = re.compile("([\(|\s]select\s)", re.IGNORECASE)
		fragSep = separator.split(s)
		if len(fragSep) == 1:
			return (0, len(s))

		fragments = [fragSep[0]] + \
			[fragSep[2*i+1] + fragSep[2*i+2] for i in xrange(len(fragSep)/2)]
						
		nesting = map(self.parenCount, fragments)
		
		#the SELECT fragment that closes the most parens is the most nested
		#the first SELECT is not ok
		minNest = min(nesting[1:])
		minPos = 1 + nesting[1:].index(minNest)
		mostNested = fragments[minPos]
		
		start = sum(map(len, fragments[:minPos]))
		end = self.getSubselectLen(fragments[minPos])
				
		return (start, start + end)
		
	def getSqlStack(self, s):		
		sqlStack = []
		tmp = s
		start = -1
		i = 1 # start numbering subselects from 1. 0 reserved for outermost
		while start != 0:
			(start, end) = self.getMostNested(tmp)
			sqlStack.append( (i, tmp[start:end] ) )
			tmp = tmp[:start] + ' [ %d ] '% i + tmp[end:]
			
			i += 1			

		#replace clusterNr of main SQL = 0
		mainFrame = sqlStack[-1]
		mainFrame = (0, mainFrame[1])
		sqlStack[-1] = mainFrame
		return sqlStack
		
		
	def massageTableCol(self, tc, tableAliases):
		t = tc.split('.')
		if len(t) == 2:
			return t[0].upper() + ':' + t[1].lower()
		else:
			#if there is one table, prefix column with table
			if len(t) == 1 and len(tableAliases) == 1:
				a = tableAliases.keys()[0]
				try:
					aliases = list(tableAliases.values()[0])
					a = [i for i in aliases if not(i.startswith(SUBSELECT))][0]
				except:
					pass
				
				return a.upper() + ':' + tc.lower()
				
		return tc

		
	"""
	[correlated] sub query
	select * from t where t.a in (select b from innerT )
	
	two steps:
	-join from parent_table.field -> workaround_1 (here)
	-replace workaround_1 with correct projectionCol"""	
	def getMainSubJoinEdges(self, s, clusterNr, tableAliases):
		res = []
						
		#sql not yet converted to lower case
		for m in re.finditer(reInSubselect, s):
			res.append('%s%s:e -> workaround_%s ;' % \
				(formatCluster(clusterNr),
				self.massageTableCol(m.group('col'), tableAliases), 
				m.group('nr')))
														
		return res
			
	def fixMainSubJoinEdges(self, mainEdges, projectionCols):
		res = []
		
		for l in mainEdges:
			parts = l.split(' -> ')
			assert len(parts) == 2
			assert parts[1][-1] == ';'
			workaroundParts = parts[1].split('workaround_')
			assert len(workaroundParts) == 2
			
			w = int(workaroundParts[1][:-1])
			assert w > 0
			
			try:
				fix = projectionCols[w].pop()
				fixParts = fix.split('.')
				assert len(fixParts) == 2
				fix = SUBSELECT + '_' + repr(w) + '_' + fixParts[0].upper() + \
					':' + fixParts[1].lower() + ':w'
			except:
				fix = SUBSELECT + '_' + repr(w) + '__dummy'						
			
									
			#compar or IN
			res.append('%s -> %s [label="IN"];' % (parts[0], fix ) )
			
		return res
		
	def process(self, s, algo):
		#"inject" tables of parent into subselects
		parentTables = {}
		#additonally keep a stack in wich frame the parentTables were
		parentTablesStack = {}
		#join outermost select with subselects
		mainEdges = []
		sqlStack = self.getSqlStack(s)
		#start with outermost select
		sqlStack.reverse()
		
		res = []
		joinsFromParent = []
		joinsFromSubselects = []
		
		projectionCols = [set() for x in xrange(len(sqlStack))]
		
		clusters = []
		i = len(sqlStack)
		for frame in sqlStack:
			(clusterNr, x) = frame
			if x[0] == '(' and x[-1] == ')':
				x = x[1:-1]
			if frame is sqlStack[0]:
				#main SQL statement
				
				#((graph_as_dot, edges_coming_from_outer_select), projectionCols)
				tmp = simpleQuery2Dot(x, 0, {}, parentTables)
				
				nicerMainGraph = subGraphDotRunner(tmp[0][0], algo)
				assert tmp[0][1] == []
				res = nicerMainGraph.strip()
				res = res.replace('digraph G', 'digraph G {\nsubgraph cluster_main ')
				res += '\n}'
			else:
				#sub-selects

				tmp = simpleQuery2Dot(x, clusterNr, parentTables, parentTables)
				projectionCols[clusterNr] = tmp[1]
				#make it a graph for gvpr processing
				
				#((graph_as_dot, edges_coming_from_outer_select), projectionCols)
				nicerSubgraph = subGraphDotRunner(
					tmp[0][0].replace('subgraph', 'graph'), algo)
					
				#change it back to subgraph
				nicerSubgraph = nicerSubgraph.replace('digraph G', 
					'subgraph cluster_%d ' % clusterNr)
				clusters.append(nicerSubgraph)
				joinsFromParent.extend(tmp[0][1])				
				
			joinsFromSubselects.extend( tmp[0][2] )				
			mainEdges.extend(self.getMainSubJoinEdges(x, clusterNr, tmp[2]))
			
		mainEdges = self.fixMainSubJoinEdges(mainEdges, projectionCols)
			
		assert res[-1] == '}'
		res = res[:-1]
		res += '\n'.join(clusters)
		res += '\n'*5
		res += '\n'.join(joinsFromParent).replace('--', '->')
		res += '\n'.join(mainEdges)
		res += '\n'.join(joinsFromSubselects)
		res += '\n}'
		return res

		
"""copied from Dive into Python / Mark Pilgrim"""
def openAnything(source):
	if source == '-':
		return sys.stdin

   	#no sense to open an URL or FTP ..
	#try opening a file
	try:
		return open(source)
	except (IOError, OSError):
		pass

	#treat source as string
	import StringIO
	return StringIO.StringIO(str(source))

def simpleQuery2Dot(s, clusterNr = 0, parentTables = {}, resultTables = {}):
	si = Simplifier()
	qr = QuoteRemover()
	#param is for quoted consts; those change after each process()
	ss = SingleSelect(qr)
	dot = DotOutput(ss)

	resultParentTables = \
		ss.process(
			si.process(
				qr.process(s)), parentTables)
	res = dot.process(clusterNr)
	
	for (k,v) in resultParentTables.items():
		subsel = '%s%d' % (SUBSELECT, clusterNr)
		v.add(subsel)
		resultTables[k] = v
		
	#((graph_as_dot, edges_coming_from_outer_select), projectionCols, tableAliases)
	return (res, ss.projectionCols, ss.tableAliases)
	
#there are similar functions in gui.py / webrevj.py
#those are rendering the final image .. 
def subGraphDotRunner(dot, algo):
	dotFile = tempfile.mkstemp('.dot', '', STATICDIR)
	os.write(dotFile[0], dot)
	os.close(dotFile[0])
	
	resFile = tempfile.mkstemp('.dot', '', STATICDIR)
	os.close(resFile[0])
		
	#cmd = '%s "%s"| gvpr -f"%s" -o"%s"'  % (algo, dotFile[1], DIRG, resFile[1])
	cmd = '%s "%s"| gvpr -f"%s" > "%s"'  % (algo, dotFile[1], DIRG, resFile[1])
	os.system(cmd)

	f = open(resFile[1])
	res = f.readlines()
	f.close()	
	
	os.remove(dotFile[1])
	os.remove(resFile[1])
	
	assert len(res) > 3
	
	return ''.join(res)


#main entry point for GUI
def query2Dot(s, algo = DEFALGO):
	#make selects lowercase, let the other classes handle the rest
	s = s.strip()
	s = re.sub('\(\s*[sS][eE][lL][eE][cC][tT]', '(select', s)
	s = re.sub('^[sS][eE][lL][eE][cC][tT]', 'select', s)

	sas = SelectAndSubselects()
	return sas.process(s, algo)

if __name__ == '__main__':
	initGlobalGrammar()
	if len(sys.argv) > 1:
		fileLike = openAnything(sys.argv[1])
		s = fileLike.read()
		print query2Dot(s)
	else:
		import unittest
		exec open('tests.py').read()
		unittest.main()
