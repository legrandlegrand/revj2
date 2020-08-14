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

"""
Do not run this file!
Unintuitive as it is, this is "included" verbatim into revj's namespace

Run the tests with:

python revj.py
"""


#common for many tests
basicTests = ['select ((((a)))) from table;',
	'select (((a))),(b+(c+(d+3))) from table;',
	"select b || '((1))2))34' from table;",
	"""select a,'"',b,'"' from table; """,
	"""select "x", '"', "y" ,'"' from table; """,
	"""select "table"."a", "table"."b" from table;"""]

simplifyTests = """select sin(cos(a+b+sin(table."c"))) from table;
	select 2*a*2 + b.b-c/3, 2*x ||'xx' from table ttt where (m *n = 0) or (ww|| qq='mmmmmmmm');
	select a, (2*a.a) + ((b+c)+3) from table;
	select a*(-1) from table;
	select instr(a,'d') || 'dd' || nvl(b.b,'c') from table;
	select nvl(a,'d'||'cc') || 'dd' || nvl(b,2+1+3) from table;
	select nvl(a.a,b) from table;
	select nvl(a.a, (2*a.a) + ((b+c.c)+3)) from table;
	select substr('aa', 3, b) from table;
	select nvl (a), nvl ( a ), nvl (a ) from table;
	select x+y+substr(a+b+upper(d)) from table;
	select sin(cos(q + upper(lower(u)))) from table;
	select sin(cos(tan(x) + y)*2) + substr(a||b.b||upper(lower("d"))) from table;
	select 2*a + b.b+c+3, x ||'xx' from table;
	select * from table ttt where (m +cos(n) = 0) or (ww || concat(qq,'xx')='mmmmmm')
	select "table2"."column name" from "schema"."table";
	select table.a_a01+99, table.a_a02*99,'*', table.b_b02+'not defined', table.c_c02+'cc' + 'dd' from table where '*'='*' or table.aa04='d';
	select 2*q*4, table.*, 2*xx from table;
	select count(*)*3 from table;
	select a + 2 + pi()*3 from table where b=random();
	select * from table order by 10*sin(cos(c33)+d44)+3;
	select * from table where a=1 or sin(b)=0.5;
	select * from table where a<>'lala'; """\
	.splitlines()

outerJoinTests = ['select * from t1, t2 where t1.id1 (+) = t2.id1;',
	'select * from t1, t2 where t1.id2 (+)= t2.id2;',
	'select * from t1, t2 where t1.id3 = (+) t2.id3;',
	'select * from t1, t2 where t1.id4 =(+) t2.id4;',
	'select * from t1, t2 where t1.id5 = ( + ) t2.id5;']

class SanityCheckerTestCase(unittest.TestCase):
	def setUp(self):
		self.sc = SanityChecker()

	def testCheckParens(self):
		self.assertRaises(BadParensException, self.sc.checkParens,
			'select ((((a))))) from table')

		assert self.sc.checkParens('select (((a))),(b+(c+(d+3))) from t;')

class QuoteRemoverTestCase(unittest.TestCase):
	def setUp(self):
		self.qr = QuoteRemover()
		
	def testRemoveUnknownMS(self):
		s = 'select * from table as t1 WITH( nolock) where x=1'
		res = self.qr.process(s)
		assert 'with' not in res
		assert 'nolock' not in res

		
	def testRemoveUnknownMS(self):
		s = 'select * from table as t1 WITH( nolock) where x=1'
		res = self.qr.process(s)
		assert 'with' not in res
		assert 'nolock' not in res
		
	def testRemoveUnknownPG(self):
		s = 'select t.aa::varchar( 20), bb[3], cc[4:5], dd[6][7] as [ee] from table t'
		res = self.qr.process(s)
		assert '::' not in res
		assert 'varchar' not in res
		assert '20' not in res
		assert 'aa' in res
		assert 'bb' in res
		assert 'cc' in res
		assert 'dd as ee' in res
		assert ':' not in res
		assert '[3]' not in res
		assert '[4:5]' not in res
		assert '[6]' not in res
		
	def testRemoveCast(self):
		s = """select  cast(T2.a as integer) 
			from T2
			group by  cast(T2.a as integer)"""
			
		res = self.qr.process(s)
		
		assert 'integer' not in res
		assert ' as ' not in res

	def testKeepQuotedUnknownPG(self):
		s = "select * from table where x='t.aa::varchar( 20)';"
		res = self.qr.process(s)
		assert '::' not in res
		assert 'varchar' not in res
		assert '20' not in res
		assert 'aa' not in res

	def testQuoteEscapes(self):
		res = [""" "my table"."my column\" is "" long" """,
			""" 'a''b\'c\'d' """]
		res = map(self.qr.removeQuoteEscapes, res)
		for r in res:
			for q in ESCAPEDQUOTES:
				assert q not in res

	def testRemoveDoubleQuotes(self):
		res = [""" "my table"."my column\" is "" long '' """,
			""" 'a''b\'c\'d' """]
		res = map(self.qr.removeQuoteEscapes, res)
		res = map(self.qr.removeQuotedIdent, res)
		for r in res:
			for q in ESCAPEDQUOTES:
				assert q not in res + [' ']

	def removeSpaces(self, x):
		return re.sub('\s+', '', x)

	def testRemoveConst(self):
		res = ["select a * 2.3E4, b * 2e4, c * .3e4 + 3.14 from table;",
			"select a || 'sin(cos(x))', '*', '1234' from table;",
			"select a || '(((' || 'dd'||'select ' from table where b='))';"]
		bads = ['sin', 'cos', '2.3', 'e4', 'E4', '2.3', '3.14',
			'((', '))', 's4']
		for r in res:
			self.qr.reset()
			x = self.qr.removeConst(r)
			for b in bads:
				try:
					assert b not in x
				except AssertionError:
					print >>sys.stderr, '[%s] in [%s]' %(b, x)
					assert b not in x

				assert x.islower()

			assert self.removeSpaces(self.qr.restoreConst(r)) == \
				self.removeSpaces(r)

	def testResetInProcess(self):
		self.qr.quotedConsts = {1:1, 2:2}
		x = self.qr.process('select a from table')
		assert self.qr.quotedConsts == {}

	def testSanityInProcess(self):
		bads = basicTests

		for r in bads:
			bads = self.qr.process(r)

		#bad SQL
		bads = ['select ((((a))))) from table;',
			'(select a+(b+(c+(d+3) from table;',
			"select a || '((1))2))34' from table;)",
			"select [a from table;",
			"select a] from table;",
			"select {a from table;",
			"select a} from table;",
			"select a' from table;",
			'select a" from table;']

		for r in bads:
			self.assertRaises(BadParensException,
				self.qr.process, r)

		#bad sql
		bads = ["""select "table"."a'", "table"."b'" from table; """,
			"""select "table"."a)" from table; """ ,
			"""select "table"."a+" from table; """]
		for r in bads:
			self.assertRaises(BadIdentException,
				self.qr.process, r)
				
	def testSanityOk(self):
		goods = ["select * from t where x='((((('",
			"select * from t where x=']]]]]]'",
			"select * from t where x='}}}}'",
			"""select * from t where x='"' """]
		for r in goods:
			bads = self.qr.process(r)

	def testReplacedConsts(self):
		s = "select m99 from t where a11 = 'aa' and t.b22 = 2e34 and 3.14 = c33;"

		res = self.qr.process(s)

		assert "'aa'" in self.qr.quotedConsts
		assert "2e34" in self.qr.quotedConsts
		assert "3.14" in self.qr.quotedConsts

	def testInClause(self):
		s = ("select * from table t where t.a in (1,2,3) "
			"and t.b in ('x', 'y') and t.c='a'")
		res = self.qr.process(s)

		assert '(' not in res
		assert ')' not in res
		assert 'in (1,2,3)' in self.qr.quotedConsts
		assert "in ('x','y')" in self.qr.quotedConsts

	def testNotInClause(self):
		s = ("select * from table t where t.a not in (1,2,3) "
			"and t.b not in ('x', 'y') and t.c='a'")
		res = self.qr.process(s)
		
		ok = False
		for i in compar.searchString(res):
			if i[0] != 'not_in_equal':
				ok = True

		if not ok:
			raise Exception('NOT BETWEEN is not an operator')

		assert '(' not in res
		assert ')' not in res
		assert 'not in (1,2,3)' in self.qr.quotedConsts
		assert "not in ('x','y')" in self.qr.quotedConsts
		
	def testEmptyStringConst(self):
		s = "SELECT IF (cu.active, 'active','') from cu"
		res = self.qr.process(s)
		
		assert "'active'" in self.qr.quotedConsts
		assert "''" in self.qr.quotedConsts

	def testBetween(self):
		s = ("select table.* from table where table.a11 between 3 and 4 and table.b22 =5 " )
		res = self.qr.process(s)

		assert "table.b22" in res
		assert "5" not in res
		assert "table.b22 =  '" in res
		assert "table.a11" in res
		assert "between_equal" in res

	def testNotBetween(self):
		s = ("select table.* from table where table.a11 not between 3 and 4 and table.b22 =5 " )
		res = self.qr.process(s)

		ok = False
		for i in compar.searchString(res):
			if i[0] != 'not_between_equal':
				ok = True

		if not ok:
			raise Exception('NOT BETWEEN is not an operator')

		assert "table.b22" in res
		assert "5" not in res
		assert "table.b22 =  '" in res
		assert "table.a11" in res
		assert "not_between_equal" in res
		
	def testSelectDistinct(self):
		s = ("Select distinct a.id from a " )
		res = self.qr.process(s)

		assert "distinct" not in res.lower()
		
class SimplifierTestCase(unittest.TestCase):
	def setUp(self):
		self.si = Simplifier()
		self.qr = QuoteRemover()
		
	def process(self, s):
		self.qr.reset()
		self.si.reset()
		return self.si.process(self.qr.process(s))

	def testSelectStar(self):
		s = "select table.*, *, a * 2 * 3 from table"
		res = self.process(s)

		try:
			assert len(res.split('*')) == 3 #that's 2 occurences of *
		except AssertionError:
			print >>sys.stderr, res
			raise Exception("* is not a binop in [%s]" % s)

	def testCountStar(self):
		s = "select count(*)*3 from table;"
		res = self.process(s)

		assert "_agg" in res
		assert "(*)" in res
		assert "3" not in res


	def testNumbersInIdent(self):
		s = 'select table.a000 * 2 + b_12-3 + "c23" + d34 from table'
		res = self.process(s)

		goods = ['a000','b_12', 'c23', 'd34']
		for g in goods:
			assert g in res

	def testProcess(self):
		s = basicTests + simplifyTests
		for x in s:
			res = self.si.process(
				self.qr.process(x))
			assert "select" in res.lower()
			assert res.islower()


	def testReduceLongOperands(self):
		s = "select a||b || c || '||||||||' from table"
		res = self.process(s)

		assert '|' not in res

	def testReduceConstBinop(self):
		s = "select 2*a, 0.2/b, (222-c), 'ss'||d from table"
		res = self.process(s)

		assert '2' not in res	#remove constant binop
		assert "'s" not in res	#remove constant binop

	def testFuncsWithComma(self):
		s = "select nvl(a111 + 2222, 3333, 4444) from table;"
		res = self.process(s)

		assert 'a111' in res
		assert '2222' not in res
		assert '3333' not in res
		assert '4444' not in res
		assert 'nvl' not in res
		
	def testFuncsWithCommaJoins(self):
		s = "select * from t1, t2 where t1.a=t2.a and nvl(t1.x,0) =(+) nvl(t2.y, 0) and t1.b = t2.b;"
		res = self.process(s)

		assert 'nvl' not in res
		
	def testFuncsNoParam(self):
		s = "select a + 2 + pi()*3 from table where b=random();"
		res = self.process(s)

		assert 'pi' not in res	#pi is redundant
		assert "random" in res	#don' remove == constant func()
		
	def testFuncsAndAggregs(self):
		s = "select x, sum(t.y) from t group by x having nvl(sum(z)) > 0;"
		res = self.process(s)
		
		assert '_agg(z)' in res
		assert 'nvl' not in res
		
	def testDistinctAggregs(self):
		s = "select count(a), count(distinct b), count(DISTINCT c) from t "
		res = self.process(s)
		
		cntPos = aggregatesAsList.index('count')
		
		assert '_%s_agg(a)' % cntPos in res
		
		#one space left after removing "(DISTINCT b)"
		assert '_%s_agg( b)' % (cntPos + AGG_DISTINCT) in res 
		assert '_%s_agg( b)' % (cntPos + AGG_DISTINCT) in res 
		assert 'distinct' not in res
		
		
	def testKeepSpacing(self):
		s = "select a, (2*m) from table;"
		res = self.process(s)

		assert ('a,' in res) or ('a ,' in res)	#keep spacing

	def testWhere(self):
		s = "select a from table where (sin(a)+pi()/2)=3+random()+4;"
		res = self.process(s)

		assert ('pi' not in res) or ('2' not in res)
		assert '/' not in res
		assert '*' not in res
		assert ('3' not in res) or ('4' not in res) or ('random' not in res)


	def testAnsiJoin(self):
		s = """select a from t1 inner join t2 on (sin(t1.a)+pi()/3)
		=4+random()+5+cos(t2.b);"""
		res = self.process(s)

		assert 't1.a' in res
		assert 't2.b' in res
		assert ('pi' not in res) or ('3' not in res)
		assert '/' not in res
		assert '*' not in res
		assert ('4' not in res) or ('5' not in res) or ('random' not in res)

	def testOuterJoin(self):
		s = outerJoinTests
		for x in s:
			res = self.process(x)
			try:
				assert "#" in res.lower()
			except AssertionError:
				raise Exception("# not in [%s]" % res)

	def testMultipleSelects(self):
		s = "select * from table where table.a in (select id from country)"
		res = self.process(s)

		assert "(" in res
		assert len(s.split('select')) == 3 # that's 2 selects

	def testColAliases(self):
		s = "select a as aa, 3 as bb, 2*3*sin(c)*4 as cc from table;"
		res = self.process(s)

		assert "aa" in res
		assert "bb" in res
		assert "cc" in res
		
	def testSquareBrackets(self):
		s = "select aa as [bb] from table;"
		res = self.process(s)

		assert "aa" in res
		
	def testDollarCurlyBraces(self):
		s = "select a,b,c from ${table};"
		res = self.process(s)

		assert "$table" in res	

	def testTableAliases(self):
		s = "select * from table1 aa, table 2 bb;"
		res = self.process(s)

		assert "aa" in res
		assert "bb" in res

	def testAnsiJoinAliases(self):
		s = "select * from table1 aa inner join table2 bb on x=y;"
		res = self.process(s)

		assert "aa" in res
		assert "bb" in res

	def testGroupBy(self):
		s = 'select * from table t group by "a11", t.b22, ' + \
			'10*sin(cos(c33)+d44)+3'
		res = self.process(s)

		assert "a11" in res
		assert "t.b22" in res
		assert "c33" in res
		assert "d44" in res

		assert 'group_by' in res

	def testOrderBy(self):
		s = 'select * from table t order by "a11", t.b22, ' + \
			'10*sin(cos(c33)+d44)+3'
		res = self.process(s)

		assert "a11" in res
		assert "t.b22" in res
		assert "c33" in res
		assert "d44" in res

		assert 'order_by' in res

	def testStarComparisons(self):
		#some reportign tools generate 'select ALL' syntax
		s = "select * from t where a=1 and ('*'='*' or nvl(b,'n')='n')" + \
			" and c=0;"
		res = self.process(s)

		assert len(res.split('=')) == 4	# 3 equals, one removed :-)
		
	def testGROUP_CONCAT(self):
		#this query is on the frontpage; embarrasing
		s = """SELECT GROUP_CONCAT(film.name, SEPARATOR ', ') AS actors
			FROM film"""
			
		res = self.process(s)

		assert "separator" not in res.lower()  
		
		
class SingleSelectTestCase(unittest.TestCase):
	def setUp(self):
		self.si = Simplifier()
		self.qr = QuoteRemover()
		#param is for quoted consts; those change after each process()
		self.ss = SingleSelect(self.qr)

	def process(self, s):
		self.qr.reset()
		self.si.reset()
		self.ss.reset()
		return self.ss.process(
				self.si.process(
					self.qr.process(s)))

	def testMultipleSelects(self):
		s = "select * from table where table.a in (select id from country)"
		self.assertRaises(MultipleSelectsException, self.ss.process, s)

	def testColAliases(self):
		s = "select a as aa, 2*t.b+4 as bb, sin(c) as cc, d, 3 as e from t;"
		res = self.process(s)

		assert 'aa' in self.ss.colAliases['t.a']
		assert 'aa' not in self.ss.colAliases['t.b']
		assert 'bb' in self.ss.colAliases['t.b']
		assert 'cc' in self.ss.colAliases['t.c']
		
		assert 't.d' not in self.ss.colAliases
		assert 'd' not in self.ss.colAliases
		assert 'as' not in self.ss.colAliases
		assert 't.e' not in self.ss.colAliases
		assert 'e' not in self.ss.colAliases

		assert 't.a' in self.ss.columns
		assert 't.b' in self.ss.columns
		assert 't.c' in self.ss.columns
		assert 't.d' in self.ss.columns

		assert 't.e' in self.ss.columns

	def testColAliasExpressions(self):
		s = 'select a, 2*(m+"n"+3) as oo ,b as bb from table;'
		res = self.process(s)

		assert 'bb' in self.ss.colAliases['table.b']
		assert 'm' not in self.ss.colAliases
		assert 'n' not in self.ss.colAliases
		assert 'table.a' in self.ss.columns
		assert 'table.b' in self.ss.columns
		assert 'table.m' in self.ss.columns
		assert 'table.n' in self.ss.columns

		assert '2' not in self.ss.columns
		assert '3' not in self.ss.columns
		assert 'table.2' not in self.ss.columns
		assert 'table.3' not in self.ss.columns

		#oo is no longer expression alias; it is a column
		assert 'table.oo' in self.ss.columns
		
	def testColAliasExpressionsWithJoins(self):
		s = 'select 2*(t1.m+"t2.n"+3) as oo ,t1.b as bb from t1, t2;'
		res = self.process(s)

		assert 'bb' in self.ss.colAliases['t1.b']
		assert 'oo' in self.ss.exprAliases

	def testStarColAlias(self):
		s = "select a, table.* from table;"
		res = self.process(s)

		assert 'table.*' in self.ss.columns
		assert '*' not in self.ss.columns
		assert 'table.a' in self.ss.columns
		assert self.ss.selectStar == False

	def testSelectStar_X(self):
		s = "select t2.a, * from table1, table2 t2, table2 t2again;"
		res = self.process(s)

		assert '*' not in self.ss.columns
		assert self.ss.selectStar == True
		assert 'table1.*' in self.ss.columns
		assert 't2.*' in self.ss.columns
		assert 't2again.*' in self.ss.columns

	def testNaturalJoin(self):
		s = "select student_name, class_name from student natural join class"

		self.assertRaises(NaturalJoinException, self.ss.process, s)

	def testOldJoin(self):
		s = "select t1.m99 from table1 t1, table2, table3 t3 where t1.a11=t3.b22"
		res = self.process(s)

		assert 'a11' not in self.ss.tableAliases
		assert 't1.a11' not in self.ss.tableAliases
		assert 'b22' not in self.ss.tableAliases
		assert 't3.b22' not in self.ss.tableAliases
		assert 'm99' not in self.ss.tableAliases
		assert 't1.m99' not in self.ss.tableAliases
		assert 'table1' in self.ss.tableAliases
		assert 'table2' in self.ss.tableAliases
		assert 'table3' in self.ss.tableAliases

		assert 't1' in self.ss.tableAliases['table1']
		assert 't2' not in self.ss.tableAliases['table1']
		assert 't3' not in self.ss.tableAliases['table1']

		assert 't3' in self.ss.tableAliases['table3']

		assert 't1.a11' in self.ss.joins
		assert 't3.b22' in self.ss.joins['t1.a11']

	def testAnsiJoin(self):
		s = """select * from (table1 t1 inner join table2 t2 on
			"t1"."a" = t2.m and sin(t1.b) = t2.n+2)
			inner join table3 t3 on t1.a = 10*t3.x and t1.b || 't1.xx' = t3.y
			where t1.xxx=1;"""
		res = self.process(s)

		assert 't1.a' in self.ss.columns
		assert 't1.b' in self.ss.columns
		assert 't2.m' in self.ss.columns
		assert 't2.n' in self.ss.columns
		assert 't3.x' in self.ss.columns
		assert 't3.y' in self.ss.columns
		assert 't1.xxx' in self.ss.columns

		assert 't1.a' in self.ss.joins
		assert 't2.m' in self.ss.joins['t1.a']
		assert 't3.x' in self.ss.joins['t1.a']
		assert 't1.b' in self.ss.joins
		assert 't2.n' in self.ss.joins['t1.b']
		assert 't3.y' in self.ss.joins['t1.b']

		assert 'table1' in self.ss.tableAliases
		assert 'table2' in self.ss.tableAliases
		assert 'table3' in self.ss.tableAliases
		
	def testLeftJoinAnsi(self):
		s = """SELECT * FROM t1 LEFT JOIN t2 on t1.a=t2.a"""
		res = self.process(s)
		
		assert 'T1.A' in self.ss.joins
		assert 't2.a' in self.ss.joins
		
	def testRightJoinAnsi(self):
		s = """SELECT * FROM t1 right JOIN t2 on t1.a=t2.a"""
		res = self.process(s)
		
		assert 't1.a' in self.ss.joins
		assert 'T2.A' in self.ss.joins
		
	def testFullJoinAnsi(self):
		s = """SELECT * FROM t1 FULL JOIN t2 on t1.a=t2.a"""
		res = self.process(s)
		
		assert 'T1.A' in self.ss.joins
		assert 'T2.A' in self.ss.joins
		
	#def testCrossAnsi(self):
	#	s = """SELECT * FROM t1 CROSS JOIN t2 where t1.a=3 and t2.b=5"""
	#	res = self.process(s)
		
	#	assert 't1.a' in self.ss.columns
	#	assert 't2.b' in self.ss.columns
	#	assert len(self.ss.joins) == 0
	#	assert '--' not in res
		
	def testCrossJoinAlias(self):
		s = """SELECT * FROM t1 a1 CROSS JOIN t2 a2;"""
		res = self.process(s)
			
		assert 't1' in self.ss.tableAliases
		assert 't2' in self.ss.tableAliases
		#this needs to be fixed properly
		#assert 'a1' not in self.ss.tableAliases
		assert 'a2' not in self.ss.tableAliases
		assert 'a1' in self.ss.tableAliases['t1']
		assert 'a2' in self.ss.tableAliases['t2']
		
		assert len(self.ss.joins) == 0
		assert '--' not in res
		
	def testJoinUsing(self):
		s = """SELECT * FROM t1 JOIN t2 USING (a,b,c,d)"""
		res = self.process(s)
		
		assert 't1.a' in self.ss.joins
		assert 't1.b' in self.ss.joins
		assert 't1.c' in self.ss.joins
		assert 't1.d' in self.ss.joins
		
		assert 't2.a' in self.ss.joins
		assert 't2.b' in self.ss.joins
		assert 't2.c' in self.ss.joins
		assert 't2.d' in self.ss.joins
		
		assert 't2.a' in self.ss.joins['t1.a']
		assert 't2.b' in self.ss.joins['t1.b']
		assert 't2.c' in self.ss.joins['t1.c']
		assert 't2.d' in self.ss.joins['t1.d']
		
		assert len(self.ss.tableAliases) == 2
		assert 't1' in self.ss.tableAliases
		assert 't2' in self.ss.tableAliases
		
	def testLeftJoinUsing(self):
		s = """SELECT * FROM t1 LEFT JOIN t2 USING (a,b,c,d)"""
		res = self.process(s)
		
		assert 'T1.A' in self.ss.joins
		assert 't2.a' in self.ss.joins
		
	def testRightJoinUsing(self):
		s = """SELECT * FROM t1 right JOIN t2 USING (a,b,c,d)"""
		res = self.process(s)
		
		assert 't1.a' in self.ss.joins
		assert 'T2.A' in self.ss.joins
		
	def testFullJoinUsing(self):
		s = """SELECT * FROM t1 FULL JOIN t2 USING (a,b,c,d)"""
		res = self.process(s)
		
		assert 'T1.A' in self.ss.joins
		assert 'T2.A' in self.ss.joins
		
	def testMax2TablesUsing(self):
		
		s = """SELECT * FROM t1 JOIN t2 USING (a,b) join t3 using (a,b)"""
		self.assertRaises(AmbiguousColumnException, self.process, s)

		
	def testOldJoinWithWhere(self):
		s = """select * from t1, t2 where  t1.id1 = t2.id2;"""
		res = self.process(s)

		assert 't2.id2' not in self.ss.filters
		
	def testAnsiMixedJoin(self):
		s = """select * from table1 t1, table2 t2 inner join table3 t3 on
			t2.id2 = t3.id3
			where t1.id1 = t2.id2"""
			
		res = self.process(s)

		assert 't2.id2' not in self.ss.tableAliases
		assert 't3.id3' not in self.ss.tableAliases
		assert 'T2.ID2' not in self.ss.tableAliases
		assert 'T3.ID3' not in self.ss.tableAliases

		assert 't1.id1' in self.ss.columns
		assert 't2.id2' in self.ss.columns
		assert 't3.id3' in self.ss.columns

		assert 't1.id1' in self.ss.joins
		assert 't2.id2' in self.ss.joins
		assert 't3.id3' in self.ss.joins

		assert 'table1' in self.ss.tableAliases
		assert 't1' in self.ss.tableAliases['table1']
		assert 'table2' in self.ss.tableAliases
		assert 't2' in self.ss.tableAliases['table2']
		assert 'table3' in self.ss.tableAliases
		assert 't3' in self.ss.tableAliases['table3']
		
	def testAnsiMixedJoinNotOrdered(self):
		s = """select * from table1 t1, table2 t2 inner join table3 t3 on
			t2.id2 = t3.id3, table4, table5 t5, table6
			where t1.id1 = t2.id2"""
			
		res = self.process(s)

		#same as previously
		assert 't2.id2' not in self.ss.tableAliases
		assert 't3.id3' not in self.ss.tableAliases
		assert 'T2.ID2' not in self.ss.tableAliases
		assert 'T3.ID3' not in self.ss.tableAliases

		assert 't1.id1' in self.ss.columns
		assert 't2.id2' in self.ss.columns
		assert 't3.id3' in self.ss.columns

		assert 't1.id1' in self.ss.joins
		assert 't2.id2' in self.ss.joins
		assert 't3.id3' in self.ss.joins

		assert 'table1' in self.ss.tableAliases
		assert 't1' in self.ss.tableAliases['table1']
		assert 'table2' in self.ss.tableAliases
		assert 't2' in self.ss.tableAliases['table2']
		assert 'table3' in self.ss.tableAliases
		assert 't3' in self.ss.tableAliases['table3']
		#new tests
		assert 'table4' in self.ss.tableAliases
		assert 'table5' in self.ss.tableAliases
		assert 't5' in self.ss.tableAliases['table5']
		assert 'table4' in self.ss.tableAliases		
		
	def testAnsiJoinWithWhere(self):	
		# from SQLite test suite
		s = """select * from t1 left join t2 on t1.b=t2.x and t1.c=1
                     left join t3 on t1.b=t3.p where t1.c=2"""								
		res = self.process(s)
		
		assert 't1' in self.ss.tableAliases
		assert 't2' in self.ss.tableAliases
		assert 't3' in self.ss.tableAliases

	def testTableAsAlias(self):
		#MySQL speciffic ?
		s = """select * from table1 as t1 inner join table2 as t2 on t1.x=t2.y;"""
		res = self.process(s)

		assert 'table1' in self.ss.tableAliases
		assert 't1' in self.ss.tableAliases['table1']
		assert 'table2' in self.ss.tableAliases
		assert 't2' in self.ss.tableAliases['table2']

	def testAnsiJoinsNoAlias(self):
		s = """select * from category left join film_category on category.category_id = film_category.category_id;"""
		res = self.process(s)
		assert 'category' in self.ss.tableAliases
		assert 'film_category' in self.ss.tableAliases

	def testGroupBy(self):
		s = 'select m99 from table t group by "a11", t.b22, ' + \
			'10* sin(cos(c33)+d44)+3'
		res = self.process(s)

		assert 't.a11' in self.ss.columns
		assert 't.b22' in self.ss.columns
		assert 't.c33' in self.ss.columns
		assert 't.d44' in self.ss.columns

		assert 't.a11' in self.ss.groups
		assert 't.b22' in self.ss.groups
		assert 't.c33' in self.ss.groups
		assert 't.d44' in self.ss.groups
		
		assert 'm99' not in self.ss.groups
		assert 't.m99' not in self.ss.groups

	def testOrderBy(self):
		s = 'select m99 from t order by "a11", t.b22, 10*sin(cos(c33)+d44)+3'
		res = self.process(s)

		assert 't.a11' in self.ss.columns
		assert 't.b22' in self.ss.columns
		assert 't.c33' in self.ss.columns
		assert 't.d44' in self.ss.columns
		
		assert 't.a11' in self.ss.orders
		assert 't.b22' in self.ss.orders
		
		assert 'm99' not in self.ss.orders
		assert 't.m99' not in self.ss.orders
		
	def testOrderByAlias(self):
		s = 'select t1.m99 as zz from t1, t2 order by zz'
		res = self.process(s)

		assert 't1.m99' in self.ss.columns	
			
		assert 'm99' not in self.ss.orders
		assert 't1.m99' not in self.ss.orders		
		assert 'zz' in self.ss.orders


	def testSumGroupHaving(self):
		s = 'select c33, d44, sum(a11), min(t.b22) from t ' + \
			'group by c33, d44 where m99=0 having 0<sum(e55)'
		res = self.process(s)

		assert 't.a11' in self.ss.columns
		assert 't.a11' in self.ss.aggregs
		assert 'sum' in self.ss.aggregs['t.a11']
		assert 't.a11' not in self.ss.groups

		assert 't.b22' in self.ss.columns
		assert 't.b22' in self.ss.aggregs
		assert 'min' in self.ss.aggregs['t.b22']
		assert 't.b22' not in self.ss.groups

		assert 't.c33' in self.ss.columns
		assert 't.c33' not in self.ss.aggregs
		assert 't.c33' in self.ss.groups

		assert 't.d44' in self.ss.columns
		assert 't.d44' not in self.ss.aggregs
		assert 't.d44' in self.ss.groups

		assert 't.e55' in self.ss.columns
		assert 't.e55' in self.ss.havings
		#need to reverse comparison operator
		assert "sum(e55)> 0" in self.ss.havings['t.e55']

		assert 't.e55' not in self.ss.aggregs
		assert 't.e55' not in self.ss.groups

		assert 't.m99' in self.ss.columns
		assert 't.m99' not in self.ss.aggregs
		assert 't.m99' not in self.ss.groups
		
	def testSumGroupHaving(self):
		s = 'select sum(a11) from t;'
		res = self.process(s)

		assert 't.a11' in self.ss.columns
		assert 't.a11' in self.ss.aggregs
		assert 'SUM(a11)' in self.ss.aggregs['t.a11']

	def testOrderBy(self):
		s = 'select m99 from t order by "a11", t.b22, 10*sin(cos(c33)+d44)+3'
		res = self.process(s)

		assert 't.a11' in self.ss.columns
		assert 't.a11' in self.ss.orders
		assert 't.b22' in self.ss.columns
		assert 't.b22' in self.ss.orders
		assert 't.c33' in self.ss.columns
		assert 't.c33' in self.ss.orders
		assert 't.d44' in self.ss.columns
		assert 't.d44' in self.ss.orders
		
	def testOrderBy(self):
		s = 'select m99 from t order by "a11", t.b22 DESC, 10*sin(cos(c33)+d44)+3 DESC'
		res = self.process(s)

		assert 't.a11' in self.ss.columns
		assert 't.a11' in self.ss.orders
		assert 't.b22' in self.ss.columns
		assert 't.b22' + '_'*10 in self.ss.orders
		assert 't.c33' in self.ss.columns
		assert 't.c33' in self.ss.orders
		assert 't.d44' in self.ss.columns
		assert 't.d44' in self.ss.orders	
	
	def testStarOrderBy(self):
		s = 'SELECT * from t order by y'
		res = self.process(s)

		assert 't.y' in self.ss.columns
		assert 't.y' in self.ss.orders		

	def testFilter(self):
		s = "select m99 from t where (a11 = 0 or a11 = 1 ) and " + \
			"  'qqq' =t.b22 and c33=pi();"
		res = self.process(s)

		assert 't.a11' in self.ss.filters
		assert '= 0' in self.ss.filters['t.a11']
		assert '= 1' in self.ss.filters['t.a11']

		assert 't.b22' in self.ss.filters
		assert "= 'qqq'" in self.ss.filters['t.b22']

		assert 't.c33' in self.ss.filters
		assert '= pi()' in repr(self.ss.filters['t.c33'])

		assert 'm99' not in self.ss.filters
		assert 't.m99' not in self.ss.filters

	def testReverseOrderFilter(self):
		s = "select * from t where 0 < x;"
		res = self.process(s)

		assert 't.x' in self.ss.filters
		#need to reverse comparison operator
		assert '> 0' in self.ss.filters['t.x']

	def testFuncOverSum(self):
		#func is not reduced
		s = "select t.x, sum(t.y) from t group by x having nvl(sum(t.z),0) > 0;"
		res = self.process(s)
		assert 't.z' in self.ss.havings
		assert "SUM(t.z)> 0" in self.ss.havings['t.z']
		
	def testSanityCheckTables(self):
		s = "select m99 from unknown_table where (a11 = 0 or a11 = 1 ) and " + \
			"  'qqq' = b.b22 and c33=pi();"

		self.assertRaises(Exception, self.ss.process,
			self.si.process(self.qr.process(s)) )

	def testEOLComments(self):
		s = """select aa from --bla bla
			table --bla"""
		res = self.process(s)
		assert 'table.aa' in self.ss.columns
		
	def testMultilineComments(self):
		s = """select aa /*, bb, cc,
			dd */ from 
			table a"""
		res = self.process(s)
		assert 'a.aa' in self.ss.columns
		assert 'a.bb' not in self.ss.columns
		assert 'cc' not in self.ss.columns
		assert 'dd' not in self.ss.columns
		assert 'a.cc' not in self.ss.columns
		assert 'a.dd' not in self.ss.columns
		
	def testMultipleComments(self):
		s = """select a.aa /*, bb, cc,
			dd */ from 
			table_a a /* nonono */, b /*hahaha*/, c"""
		res = self.process(s)
		assert 'a.aa' in self.ss.columns
		assert 'a.bb' not in self.ss.columns
		assert 'cc' not in self.ss.columns
		assert 'dd' not in self.ss.columns
		assert 'a.cc' not in self.ss.columns
		assert 'a.dd' not in self.ss.columns
		
		assert 'table_a' in self.ss.tableAliases
		assert 'a' in self.ss.tableAliases['table_a']
		assert 'b' in self.ss.tableAliases
		assert 'c' in self.ss.tableAliases
		assert 'nonono' not in self.ss.tableAliases
		assert 'a.nonono' not in self.ss.columns
		assert 'b.nonono' not in self.ss.columns
		assert 'c.nonono' not in self.ss.columns
		assert 'hahaha' not in self.ss.tableAliases
		assert 'a.hahaha' not in self.ss.columns
		assert 'b.hahaha' not in self.ss.columns
		assert 'c.hahaha' not in self.ss.columns
		
	def testOraOuterJoins_l(self):
		s = "select * from t1, t2 where  t1.id (+) = t2.id ;"
		res = self.process(s)

		assert "T1.ID" in self.ss.joins
		assert "t2.id" in self.ss.joins

	def testOraOuterJoins_r(self):
		s = "select * from t1, t2 where t1.id = t2.id (+);"
		res = self.process(s)

		assert "t1.id" in self.ss.joins
		assert "T2.ID" in self.ss.joins
		
	def testFullOuterJoins_r(self):
		s = "select * from t1 full outer join t2 on t1.id = t2.id"
		res = self.process(s)

		assert "T1.ID" in self.ss.joins
		assert "T2.ID" in self.ss.joins

	def testLeftOuterJoins(self):
		s = "select * from t1 left outer join t2 on t1.id = t2.id ;"
		res = self.process(s)

		assert "T1.ID" in self.ss.joins
		assert "t2.id" in self.ss.joins

	def testRightOuterJoins(self):
		s = "select * from t1 right outer join t2 on t1.id = t2.id ;"
		res = self.process(s)

		assert "t1.id" in self.ss.joins
		assert "T2.ID" in self.ss.joins

	def testInClause(self):
		s = ("select * from table t where t.a in (1,2,3) "
			"and t.b in ('x', 'y') and t.c='a';" )
		res = self.process(s)

		assert "t.a" in self.ss.filters
		assert "t.b" in self.ss.filters
		assert "t.c" in self.ss.filters
		
	def testNotInClause(self):
		s = ("select * from table t where t.a not in (111,222,333) "
			"and t.b not in ('xxx', 'yyy') and t.c='a';" )
		res = self.process(s)

		assert "t.a" in self.ss.filters
		assert "t.b" in self.ss.filters
		assert "t.c" in self.ss.filters

	def testLIKE(self):
		s = ("select table.* from table where table.a LIKE '%qqq%' " )
		res = self.process(s)
		
		assert "table.a" in self.ss.filters
		assert "like '%qqq%'" in self.ss.filters['table.a']
		assert "table.*" in self.ss.columns

	def testNotLIKE(self):
		s = ("select table.* from table where table.a NoT liKE '%qqq%' " )
		res = self.process(s)
		
		assert "table.a" in self.ss.filters
		assert "not like '%qqq%'" in self.ss.filters['table.a']
		
	def testISNULL(self):
		s = ("select table.* from table where table.a IS NulL " )
		res = self.process(s)

		assert 'table.a' in self.ss.filters
		assert 'is null' in self.ss.filters['table.a']

	def testISNOTNULL(self):
		s = ("select table.* from table where table.a is not null " )
		res = self.process(s)

		assert 'table.a' in self.ss.filters
		assert 'is not null' in self.ss.filters['table.a']

	def testBetween(self):
		s = ("select table.* from table where table.a between 1 and 2 and table.b =3 " )
		res = self.process(s)

		assert "table.b" in self.ss.filters
		assert "= 3" in self.ss.filters['table.b']
		assert "table.a" in self.ss.filters
		assert "between_equal" in list(self.ss.filters['table.a'])[0]
		assert "1 and 2" in list(self.ss.filters['table.a'])[0]
		#bugfix for double quoting
		assert "1 and 2'" not in list(self.ss.filters['table.a'])[0]

		assert 'between_equal' not in res
		assert 'table.between_equal' not in self.ss.columns
		assert 'between_equal' not in self.ss.columns
		
	def _testExprBetween(self):
		s = ("select table.* from table where a between 1 and f(3)" )
		res = self.process(s)

		assert 'table.a' in self.ss.filters
		
		assert 'between' not in self.ss.columns
		assert 'table.between' not in self.ss.columns
		assert 'between_equal' not in res
		assert 'table.between_equal' not in self.ss.columns
		assert 'between_equal' not in self.ss.columns
		
		assert '' in self.ss.filters['table.a']
		assert "between_equal" in list(self.ss.filters['table.a'])[0]
		assert "1 and " in list(self.ss.filters['table.a'])[0]
		
	def testNestedCaseWhenThen(self):
		s = """SELECT
		  aa, bb,
		  sin(cos((CASE cc
			WHEN 'M' THEN 'Male'
			WHEN 'F' THEN 'Female'
		  END)))
		FROM t """
		res = self.process(s)

		for bad in 'sin cos case when then end Male Female'.split():
			assert bad not in res
			assert bad not in self.ss.columns
			assert bad.upper() not in res
			assert bad.upper() not in self.ss.columns
			
		assert 't.aa' in self.ss.columns
		assert 't.bb' in self.ss.columns
		assert 't.cc' in self.ss.columns
		
	def testCaseWhenFallTrhu(self):
		s = "select case when a+b+c=0 then 'x' when a+b='0' then '0' end from t"
		res = self.process(s)
		
		assert 't.a' in self.ss.columns
		assert 't.b' in self.ss.columns
		assert 't.c' in self.ss.columns
		
	def testaliasSingleSelect(self):
		s = ("select * from table where a=1" )
		res = self.process(s)

		assert "table.a" in self.ss.filters
		assert "= 1" in self.ss.filters['table.a']
		
	def testS0Columns(self):
		s = "select a.s5 from a where a.s6='aaa' and a.s7='fff'"
		res = self.process(s)

		assert "s0" not in self.ss.columns
		assert "s1" not in self.ss.columns
		assert "a.s0" not in self.ss.columns
		assert "a.s1" not in self.ss.columns	
		
	def testAmbiguousColumnException(self):
		s = "select a from t1, t2"		
		self.assertRaises(AmbiguousColumnException, self.process, s)	
		
		
	def testEmptyStringConst(self):
		s = "SELECT IF (cu.active, 'active','') from cu"
		res = self.process(s)
		
		assert 'if' not in self.ss.columns
		assert 'IF' not in self.ss.columns
		
	def testDerivedTables(self):
		#SimpleSelect handles only one SELECT, simulate the rest
		s = "select w.a, t.b from [ 1 ] as w, t"
		res = self.process(s)
		
		assert 'w' in self.ss.tableAliases
		assert 'w' in self.ss.derivedTables
		assert self.ss.derivedTables['w'] == 1
		
	def testSplitByCommasWihoutParens(self):
		s = "a, b as bb, nvl(a,'b') as c, 1+f(1,1,1, g(2,2,2,d))+3 as d"
		assert( len(splitByCommasWithoutParens(s))) == 4
		assert ',' not in splitByCommasWithoutParens(s)[1]
		
		s = "b as bb"
		assert splitByCommasWithoutParens(s) == ["b as bb"]
		
	def testAliasesBelongInTables(self):
		s = "select a as aa, 2*t.b+4 as bb, sin(c) as cc, d, 3 as e from t;"
		res = self.process(s)

		assert 't.aa' in self.ss.projectionCols
		assert 't.bb' in self.ss.projectionCols
		assert 't.cc' in self.ss.projectionCols
		assert 't.d' in self.ss.projectionCols
		assert 't.e' in self.ss.projectionCols

		#aa, bb, cc are displayed "a as aa"
		assert 't.e' in self.ss.columns
		
	def testAliasesForMoreTables(self):
		pass
		
	def testMultipleFrom(self):
		s = "select a, b from t1 from t2;"
		self.assertRaises(MallformedSQLException, self.process, s)		
		
	def testReservedWordsJoin(self):
		s = "select * from t1 join t2 on t1.inner_1 = t2.left_2 join " + \
			"t3 on t2.left_2 = t3.full_3"
		res = self.process(s)
		
		assert 't1.inner_1' in self.ss.joins
		assert 't2.left_2' in self.ss.joins
		assert 't3.full_3' in self.ss.joins
		assert len(self.ss.joins) == 3
		
	def testJoinWithExtraParens(self):
		s = "select * from t1 join (t2 a2) on t1.x = a2.x join " + \
			"(t3 a3) on a2.y = a3.y"
		res = self.process(s)
		
		assert 't1.x' in self.ss.joins
		assert 'a2.x' in self.ss.joins
		assert 'a2.y' in self.ss.joins
		assert 'a3.y' in self.ss.joins	
		
	def testCountStar(self):
		s = "select count(*) from t1, t2;"
		
		# * is now a valid column, previously only t.* was ok
		res = self.process(s)
		
	def testCountDistinct(self):
		s = """SELECT count(distinct x) from t"""
		res = self.process(s)
		
		assert 't.x)' not in self.ss.columns
		
	def testSelectDistinct(self):
		s = """Select distinct a.id from a """
		res = self.process(s)
		
		assert 'a.id' in self.ss.columns
	
	def testSubselectEdges(self):
		s = """select fact.*, region.name, product.name
			from fact,
					[ 1 ] region,
					[ 2 ] product
			where 
				fact.region_id = region.id and
				fact.product_id = product.id;"""
				
		res = self.process(s)
		assert 'region' in self.ss.subselects
		assert 'product' in self.ss.subselects
		
	def testGROUP_CONCAT2(self):
		#this query is on the frontpage; embarrasing
		s = """SELECT CONCAT(a, 2 + b) FROM t"""
		
		res = self.process(s)
		
		assert 'concat' not in self.ss.columns

class DotOutputTestCase(unittest.TestCase):
	def setUp(self):
		self.si = Simplifier()
		self.qr = QuoteRemover()
		#param is for quoted consts; those change after each process()
		self.ss = SingleSelect(self.qr)
		self.dot = DotOutput(self.ss)

	def process(self, s, verbose = False):
		dummy = self.ss.process(
					self.si.process(
						self.qr.process(s)))
		if verbose:
			print '*'*5, self.si.process(self.qr.process(s))
		return self.dot.process()[0]

	def testSimple(self):
		s = "select t.m99 from table t where t.a11=1"
		res = self.process(s)

		assert 'a11 = 1' in res
		assert 'm99' in res
		assert 'T | (TABLE)' in res
		
	def testColumnAlias(self):
		s = "select t.m99 as mm from t"
		res = self.process(s)

		assert 'm99' in res
		assert '<m99>' in res
		assert 'AS mm' in res
		assert '<mm>' in res

	def testSum(self):
		s = "select sum(t.m99) from table t where t.a11=1 group by t.b22"
		res = self.process(s)

		assert 'SUM(m99)' in res
		assert 'a11 = 1' in res
		assert 'GROUP BY b22' in res

	def testJoins(self):
		s = "select t1.a11, sum(t2.b22) from table1 t1 inner join table2 t2 on t1.id = t2.id group by t1.a11"
		res = self.process(s)

		assert 'a11' in res
		assert 'SUM(b22)' in res
		assert 'GROUP BY a' in res
		assert '<id> id' in res
		assert 'T1:id' in res
		assert 'T2:id' in res

	def testSakila_1(self):
		s = """SELECT
			cu.customer_id AS ID,
			CONCAT(cu.first_name, _utf8' ', cu.last_name) AS name,
			a.address AS address,
			a.postal_code AS `zip code`,
    		a.phone AS phone,
			city.city AS city,
			country.country AS country,
			IF (cu.active, _utf8'active',_utf8'') AS notes,
			cu.store_id AS SID
		FROM
			customer AS cu JOIN address AS a ON
				cu.address_id = a.address_id
			JOIN city ON a.city_id = city.city_id
    		JOIN country ON city.country_id = country.country_id;"""
		res = self.process(s)

		assert 'A:address_id -- CU:address_id [color = black arrowtail="none"' in res
		assert 'CITY:country_id -- COUNTRY:country_id [color = black arrowtail="none"' in res
		assert 'A:city_id -- CITY:city_id [color = black arrowtail="none"' in res
		assert 'concat' not in res.lower() 

	def testPagila_1(self):
		s = """SELECT
				cu.customer_id AS id,
				(((cu.first_name)::text || ' '::text) || (cu.last_name)::text) AS name,
				a.address,
				a.postal_code AS "zip code",
				a.phone,
				city.city,
				country.country,
				CASE WHEN cu.activebool THEN 'active'::text ELSE ''::text END AS notes,
				cu.store_id AS sid
			FROM
				(((customer cu JOIN address a ON
					((cu.address_id = a.address_id)))
				JOIN city ON ((a.city_id = city.city_id)))
				JOIN country ON ((city.country_id = country.country_id)));"""
		res = self.process(s)

		assert 'A:address_id -- CU:address_id [color = black arrowtail="none"' in res
		assert 'CITY:country_id -- COUNTRY:country_id [color = black arrowtail="none"' in res
		assert 'A:city_id -- CITY:city_id [color = black arrowtail="none"' in res
		assert 'zip' in res

	def testPagila_2(self):
		#Pagila, Postgres syntax
		s="""SELECT
			film.film_id AS fid,
			film.title,
			film.description,
			category.name AS category,
			film.rental_rate AS price,
			film.length,
			film.rating,
			group_concat((((actor.first_name)::text || ' '::text) ||
				(actor.last_name)::text)) AS actors
			FROM
				((((category LEFT JOIN film_category ON
					((category.category_id = film_category.category_id)))
				LEFT JOIN film ON ((film_category.film_id = film.film_id)))
				JOIN film_actor ON ((film.film_id = film_actor.film_id)))
				JOIN actor ON ((film_actor.actor_id = actor.actor_id)))
			GROUP BY
				film.film_id, film.title, film.description, category.name, 
				film.rental_rate, film.length, film.rating;"""

		res = self.process(s)
		assert 'ACTOR:actor_id -- FILM_ACTOR:actor_id [color = black arrowtail="none"' in res
		assert ('CATEGORY:category_id -- FILM_CATEGORY:category_id [color = %s arrowtail="%s" arrowhead="none"];' % \
			(OUTERJOINCOLOR, OUTERJOINARROW) ) in res
		assert ('FILM:film_id -- FILM_CATEGORY:film_id [color = %s arrowtail="none" arrowhead="%s"];' % \
			(OUTERJOINCOLOR, OUTERJOINARROW) ) in res
		assert 'FILM:film_id -- FILM_ACTOR:film_id [color = black arrowtail="none"' in res
		assert 'actors' in res

	def testSakila_3(self):
		s = """SELECT
			c.name AS category,
			sum(p.amount) AS total_sales
			FROM
				(((((payment p JOIN rental r ON ((p.rental_id = r.rental_id)))
				JOIN inventory i ON ((r.inventory_id = i.inventory_id)))
				JOIN film f ON ((i.film_id = f.film_id)))
				JOIN film_category fc ON ((f.film_id = fc.film_id)))
				JOIN category c ON ((fc.category_id = c.category_id)))
			GROUP BY c.name ORDER BY sum(p.amount) DESC;"""

		res = self.process(s)
		assert 'P:rental_id -- R:rental_id [color = black arrowtail="none"' in res
		assert 'C:category_id -- FC:category_id [color = black arrowtail="none"' in res
		assert 'I:inventory_id -- R:inventory_id [color = black arrowtail="none"' in res
		assert 'F:film_id -- I:film_id [color = black arrowtail="none"' in res
		assert 'F:film_id -- FC:film_id [color = black arrowtail="none"' in res

	def testMondrian(self):
		s = """SELECT
            `DC`.`gender`,
            `DC`.`marital_status`,
            `DPC`.`product_family`,
            `DPC`.`product_department`,
            `DPC`.`product_category`,
            `DT`.`month_of_year`,
            `DT`.`quarter`,
            `DT`.`the_year`,
            `DB`.`customer_id`
        FROM
            `sales_fact_1997` `DB`,
            `time_by_day` `DT`,
            `product` `DP`,
            `product_class` `DPC`,
            `customer` `DC`
        WHERE
            `DB`.`time_id` = `DT`.`time_id`
        AND `DB`.`customer_id` = `DC`.`customer_id`
        AND `DB`.`product_id` = `DP`.`product_id`
        AND `DP`.`product_class_id` = `DPC`.`product_class_id`"""
		res = self.process(s)
		assert 'DB:customer_id -- DC:customer_id [color = black arrowtail="none"' in res
		assert 'DB:time_id -- DT:time_id [color = black arrowtail="none"' in res
		assert 'DP:product_class_id -- DPC:product_class_id [color = black arrowtail="none"' in res
		assert 'DB:product_id -- DP:product_id [color = black arrowtail="none"' in res


	def testDiffer(self):
		s = """select * from t where t.xxxx <> 1 """
		res = self.process(s)
		assert '\<\> 1' in res

	def testOuterJoins(self):
		#not sure it's ok to mix INNER and (+)= ...
		s = "select * from t1 inner join t2 on t1.id1 (+)= t2.id2;"
		res = self.process(s)

		assert "<id1> id1" in res
		assert "<id2> id2" in res
		assert "T1:id1 -- T2:id2" in res
		assert "T2:id2 -- T1:id1" not in res
		assert ('[color = %s arrowtail="%s" arrowhead="none"];' % (OUTERJOINCOLOR, OUTERJOINARROW) )\
				in res

	def testFullJoins(self):
		s = "select * from t1 full outer join t2 on t1.id1 = t2.id2"
		res = self.process(s)

		assert "<id1> id1" in res
		assert "<id2> id2" in res
		assert "T1:id1 -- T2:id2" in res
		assert ('arrowtail="%s" arrowhead="%s"' % (OUTERJOINARROW, OUTERJOINARROW) ) in res

	def testInClause(self):
		s = ("select * from table t where t.a11 in (1,2,3) "
			"and t.b22 in ('x', 'y')" )
		res = self.process(s)

		assert "a11" in res
		assert "b22" in res
		assert "in (1,2,3)" in res
		assert "IN IN" not in res.upper()
		
	def testInClause(self):
		s = ("select * from table t where t.a11 not in (1,2,3) "
			"and t.b22 not in ('x', 'y')" )
		res = self.process(s)

		assert "a11" in res
		assert "b22" in res
		assert "not in (1,2,3)" in res
		assert "not in ('x'"  in res
		assert "IN IN" not in res.upper()

	def testLIKE(self):
		s = ("select table.* from table where table.a11 LIKE '%qqq%' " )
		res = self.process(s)

		assert "a11" in res
		assert "like '%qqq%'" in res
		assert "*" in res
		

	def testBetween(self):
		s = ("select table.* from table where table.a11 between 1 and 2 and table.b22 =3 " )
		res = self.process(s)

		assert "b22" in res
		assert "= 3" in res
		assert "a11" in res
		assert "between 1 and 2" in res
		#bugfix
		assert "'between 1 and 2'" not in res
		
	def testDollar(self):
		s = ("select * from ${table}" )
		res = self.process(s)

		assert "TABLE | ($TABLE)" in res
		
	def testPipeCompar(self):
		s = ("select * from t where x |= 3" )
		res = self.process(s)

		assert "x \|= 3" in res
		
	def testNegativeNumber(self):
		s = ("select * from t where x = -3" )
		res = self.process(s)

		assert "x = -3" in res
		
	def testFloatNumber(self):
		s = ("select * from t where x = 3.3" )
		res = self.process(s)

		assert "x = 3.3" in res
		
	def testLessVerboseOutput(self):
		s = ("SELECT sum(t1.x) from t1, t2 where t1.x=t2.y and t2.y=3")
		res = self.process(s)

		assert "<x> SUM(x)" in res
		assert "<y> y = 3" in res
		
	def testSingleQuotedDoubleQuote(self):
		s = ("""SELECT * from t where x='quote="'; """)
		res = self.process(s)

		assert "quote=\\" in res
		
	def testSpaceInAlias(self):
		s = ("""SELECT x as "x alias" from t""")
		res = self.process(s)
		
		assert r'\"x alias\"' in res
		
	def testOrderBy(self):
		s = ("""SELECT * from t order by y, z desc""")
		res = self.process(s)
		
		assert 'ORDER BY y|' in res
		assert 'ORDER BY z DESC' in res
		
	def testCountDistinct(self):
		s = ("""SELECT count(distinct x) from t""")
		res = self.process(s)
		
		assert 'COUNT(DISTINCT x)' in res
		assert 'COUNT' in res
		
	def testHavingHasNoTableName(self):
		s = "select t.x, sum(t.y) from t group by x having nvl(sum(t.z),0) > 0;"
		res = self.process(s)

		assert 'SUM(z)' in res
		assert '_agg' not in res
		
	def testDerivedTables(self):
		s = 'select w.a, t.b from [ 1 ] as w, t where t.a=w.a'
		res = self.process(s)
		
		assert 'label="W |' not in res
		assert ('T:a --' in res) or ('-- T:a' in res)
		
	def testColAliasExpressionsWithJoins(self):
		s = 'select 2*(t1.m+"t2.n"+3) as oo ,t1.b as bb from t1, t2;'
		res = self.process(s)

		assert '... AS bb' not in res
		assert '... AS oo' in res
		
	def testShorterAliasExpressions(self):
		s = 'select t1.b+t1.m as bb from t1, t2;'
		res = self.process(s)

		assert '... AS bb' in res
		#_ is a special field meaning expression alias
		assert '_|' not in res
		
	def testNotShorterAliasExpressions(self):
		s = 'select t1.b+t1.m as bb, t1.xx as xx from t1, t2 order by t1.bb;'
		res = self.process(s)

		assert '... AS bb' in res
		assert 'ORDER BY bb' in res
		
		assert 'xx AS xx' in res
		
	def testSelectStarAlone(self):
		s = 'select * from t;'
		res = self.process(s)

		assert '*' in res
		
	def testColAliasesWithoutAS(self):
		s = 'select t1.a aa, t2.b bb, t1.a+t1.b c from t1, t2;'
		res = self.process(s)

		assert 'a AS aa' in res
		assert 'b AS bb' in res
		assert '... AS c' in res
		
	def testTrueFalse(self):
		s = "select * from t where a = true and b=false;"
		res = self.process(s)
		
		assert 'a = 1' in res
		assert 'b = 0' in res
		
	def testLeftJoinWithFilter(self):
		s = "select * from t1 left join t2 on t1.a=t2.a and t2.b=4;"
		res = self.process(s)
		
		# left join represented internally as #=
		assert 'b = 4' in res
		assert '#' not in res
		
	def testHaving(self):	
		s = "select * from t group by x having nvl(sum(z)) > 0;"
		res = self.process(s)
		
		assert r'HAVING SUM(z)\> 0' in res
		
	def testIncorrectConstantAlias(self):
		s = "select case when a='x' then 1 end from t"
		res = self.process(s)
		
		#previously alias was set to the constant
		assert QUOTESYMBOL not in res
		
		
	def testFanChasmTrap(self):
		s = """select p.product, sum(o.sales), sum(sp.sales) from 
		products p inner join orders o on p.pid = o.pid inner join 
		salesplans sp on p.pid = sp.pid group by p.pid"""
		res = self.process(s)
		
		assert "Risk of Fan and/or Chasm trap" in res
		
	def testSchemaField(self):
		s = "Select a.x, a.y, schema.B.z From a, schema.b where a.id=schema.b.id"
		res = self.process(s)
		
		assert 'SCHEMA__B |' in res
		assert '(SCHEMA.B)' in res
		assert ('A:id -- SCHEMA__B:id' in res) or ('SCHEMA__B:id -- A:id' in res)
		
	def testSchemaFieldAlias(self):
		s = "Select a.x, a.y, B.z From a, schema.b b where a.id=b.id"
		res = self.process(s)

		assert 'B |' in res
		assert '(SCHEMA.B)' in res
		assert ('A:id -- B:id' in res) or ('B:id -- A:id' in res)
		
	def testBindVariables(self):
		s = "select * from a where a = :999 and b = :xxx"
		res = self.process(s)

		assert 'a = :999' in res
		assert 'b = :xxx' in res
		
class SelectAndSubselectsTest(unittest.TestCase):
	def setUp(self):
		self.sas = SelectAndSubselects()
		
	def _testIn(self):
		s = "select ((a)) .. where x in (select 1...) and y in (select 2...)"
		(start, end) = self.sas.getMostNested(s)
		assert '(select 1...)' in s[start:end]
		
	def testNA(self):
		s = "select ((a)) .. where x =0"
		(start, end) = self.sas.getMostNested(s)
		assert s == s[start:end]
		
	def testNesting1(self):
		s = "select ((a)) from (select 1... where (select 2...))"
		(start, end) = self.sas.getMostNested(s)
		assert '(select 2...)' in s[start:end]
		
	def testNesting2(self):
		s = ("select ((a)) from (select 1... where (select 2...)) and "
			"((x in (select 3...)))")
		(start, end) = self.sas.getMostNested(s)
		assert '(select 3...)' in s[start:end]
		
	def testNesting3(self):
		s = "select ((a)) from (select 1...() where (select 2...()))"
		(start, end) = self.sas.getMostNested(s)
		assert '(select 2...())' in s[start:end]

	def testUnionAndSubselects(self):
		s = ("select ((a)) from (select 1... where (select 2...)) "
			"union select 3...")
		(start, end) = self.sas.getMostNested(s)
		assert '(select 2...)' in s[start:end]
		assert 'union' not in s[start:end]	
		
	def testUnionOnly(self):
		s = "select 1... union select 2... union select 3..."
		(start, end) = self.sas.getMostNested(s)
		assert 'select 2...' in s[start:end]
		assert 'union' in s[start:end]
		
	def testNestedUnion(self):
		s = "select 1 .. (select 2... union select 3...)"
		(start, end) = self.sas.getMostNested(s)
		assert 'select 3...' in s[start:end]
		assert 'union' not in s[start:end]
		assert ')' not in s[start:end]

	def testAliasedSubselect(self):
		s = "select 1... from table, (select 2...) as a where ..."
		(start, end) = self.sas.getMostNested(s)
		assert 'select 2...' in s[start:end]

		
	def testProcessIn(self):
		s = ("select ((a)) from (select 1... where (select 2...)) and "
			"((x in (select 3...)))")
		stack = self.sas.getSqlStack(s)
		assert '(select 3...)' in stack[0][1]
		assert '(select 2...' in stack[1][1]
		assert '(select 1... where  [ 2 ] )' in stack[2][1]
		assert 'select ((a)) from  [ 3 ]  and ((x in  [ 1 ] ))' in stack[3][1]
		
		#self.sas.process(s)
		
	def testProcessCorrelated(self):
		s = """SELECT Album.song_name FROM Album
		WHERE Album.band_name = 'Metallica'
		AND EXISTS
		(SELECT Cover.song_name FROM Cover
		WHERE Cover.band_name = 'Damage, Inc.'
		AND Cover.song_name = Album.song_name);"""
				
		res = query2Dot(s)
		assert 'ALBUM:song_name -> ___SUBSELECT____1_COVER:song_name ' in res
		
	def testCorrelatedAlias(self):
		s = "select a.a as xxx from a, (select * from b where b.b = a.xxx) "
		res = self.sas.process(s, 'neato')
		
		assert 'A:xxx -> ___SUBSELECT____1_B:b ' in res

	def testSubselect(self):
		s = "SELECT a.id from a where a.b in (select c from d) "
		res = query2Dot(s)
		assert 'workaround_' not in res
		assert '[label="IN"]' in res
		assert '<b> b' in res
		assert '<c> c' in res
		
	def testDerivedTables(self):
		s = """select dt_1.a, dt_1.b, dt_2.c
		from (select a, b from t1) 
         as dt_1,
        (select b, c from t2) 
         as dt_2
		where dt_1.b = dt_2.b"""
		res = query2Dot(s)
		
		assert "DERIVED TABLE | (DT_1)" in res
		assert "DERIVED TABLE | (DT_2)" in res
		assert "T1 | (T1)" in res
		assert "T2 | (T2)" in res
		assert "DT_1:b:e -> DT_2:b:w" in res
		
	def testSubselectEdges(self):
		s = """select fact.*, region.name, product.name
			from fact,
					(select name, id from translation where lang='EN' and type='region') region,
					(select name, id from translation where lang='EN' and type='product') product
			where 
				fact.region_id = region.id and
				fact.product_id = product.id;"""
				
		res = query2Dot(s)
		
		assert 'REGION -> ___SUBSELECT____1__dummy' in res
		assert 'PRODUCT -> ___SUBSELECT____2__dummy' in res
		
	def testSubselectPreferLocalTable(self):
		s = """select *
			from
			(select * from A,B where A.x=B.y) D1,
			(select * from A,B where A.x=B.y) D2"""
			
		res = query2Dot(s)
		
		#disregard tables A,B from the other subselect; use local ones
		assert '___SUBSELECT____2_A:x:e -> ___SUBSELECT____2_B:y:w' in res
		assert '___SUBSELECT____1_A:x:e -> ___SUBSELECT____1_B:y:w' in res
		
	def testSubselectPreferLocalAlias(self):
		s = """select *
		from
		(select * from ta A,tb B where A.x=B.y) D1,
		(select * from ta A,tb B where A.x=B.y) D2"""
		
		res = query2Dot(s)
		
		#disregard tables A,B from the other subselect; use local ones
		assert '___SUBSELECT____2_B:y:e -> ___SUBSELECT____2_A:x:w' in res
		assert '___SUBSELECT____1_B:y:e -> ___SUBSELECT____1_A:x:w' in res
		
	def testSubselectParentTables(self):
		s = """select parentP.y,q.z
			from
			 parentP,
			 parentQ q,
			 (select * from A,B where A.x=B.x and A.y=parentP.y and A.z = q.z) D1"""
			 
		res = query2Dot(s)
		
		#finds tables and aliases from parent
		assert '___SUBSELECT____1_A:y -> PARENTP:y' in res
		assert '___SUBSELECT____1_A:z -> Q:z' in res
		
	def testInSubselect(self):
		s = """select * from (select A.id from A 
		where A.id IN (select B.id from B where v=5)) D1 """
			 
		res = query2Dot(s)
		
		#finds tables and aliases from parent
		assert '___SUBSELECT____2_A:id:e -> ___SUBSELECT____1_B:id:w' in res
		
	def testLongIn(self):
		s = """SELECT * from t where x in (1,2,3,4,5,6,7,8,9 , 100 )"""
		res = query2Dot(s)
		assert '...' in res
		assert '100' not in res
		
	def testAnonSubselects(s):
		s = """select * from (select A.id from A)"""
		res = query2Dot(s)
		assert 'ANON_SUBSELECT_1 -> ___SUBSELECT____1' in res
		
	def testInSubSelectPopFromEmptySet(self):
		s = "select o.x from out o where o.y in ( select * from t1 at1)"
		res = query2Dot(s)
		assert 'O:y:e -> ___SUBSELECT____1__dummy [label="IN"]' in res
		
	def testUnion(self):
		s = """select o.x from out o where o.y in	(
				 select t1.a from t1 at1
				 uniON all
				 select t2.b from t2
				 eXcePT All
				 select t3.c from t3)"""
		res = query2Dot(s)
		assert '___SUBSELECT____3__dummy -> ___SUBSELECT____2__dummy' in res
		assert 'label="union all"' in res
		assert '___SUBSELECT____2__dummy -> ___SUBSELECT____1__dummy' in res
		assert 'label="except all"' in res
		
	def testUnionTopLevel(self):
		s = "SELECT a from A union select b from B"
		res = query2Dot(s)
		#add a cluster also for the main =top SQL frame
		assert 'subgraph cluster_main' in res
		assert '_dummy -> ___SUBSELECT____1__dummy' in res
		
	def testInSubselectWithoutTableCol(self):
		s = "SELECT * FROM A WHERE x IN  (SELECT y FROM B )"
		res = query2Dot(s)
		assert 'A:x:e -> ___SUBSELECT____1_B:y:w [label="IN"]' in res
		
	def testJoinIsNotInSubselect(self):
		s = "select * from D1 full outer join (select b from B) D2"
		res = query2Dot(s)
		#in does not match last 2 letters of join
		assert 'jo:' not in res
		assert '[label="IN"]' not in res
		
	def testCaselessUnion(self):
		s = "SELECT a FROM A UNION seLECT b FROM B"
		#usd to throw exception because select was not all lowercase
		res = query2Dot(s)
		assert 'label="union"' in res
		
	def testMoreUnions(self):
		s = """select t1.a from T1
			union all select t2.b from T2
			union all select t3.c from T3"""

		res = query2Dot(s)
		#2 UNION edges
		assert '_dummy -> ___SUBSELECT____1__dummy' in res
		assert '_dummy -> ___SUBSELECT____2__dummy' in res
		assert 'label="union all"' in res
		
		#no table alias for ALL
		assert 'ALL | (ALL)' not in res

	def testFuncsNoParens(self):
		s = "Select  t1.x, t2.y from t1, t2 where t1.z=sysdate;"
		res = query2Dot(s)
		
		assert "sysdate" in res	#don' remove == constant func()