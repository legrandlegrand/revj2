import os, sys, platform, tempfile
from string import Template
import cherrypy
import revj
from constants import *


#exacly same order as in template
algos = ['neato', 'fdp', 'twopi', 'circo', 'one-pass']

#not exact copy from gui.py
def dotRunner(algo, dist, dotFile, pngFile):
	cmd = ''
	if 'one' in algo:
		#if there is some problem in the script for gvpr
		cmd = 'dot -Grankdir=LR  -Granksep=%s -Edir=none -T png -o "%s" "%s"' % \
			(dist, pngFile, dotFile)
	else:
		cmd = ('%s "%s" | gvpr -f"%s" | dot -Grankdir=LR -Granksep=%s -Edir=none ' 
			'-T png -o "%s"') % (algo, dotFile, DIRG, dist, pngFile)
			
	os.system(cmd)
	
#generate a graph containing the Exception
def getException():
	return "%s" % sys.exc_info()[1]
	
#prepare a dictionary for template substitution
#for each OPTION in the SELECT tags there is one value='selected' OR ''
def getMapping(sql, algo, dist, png, err):
	mapping = {}
	for a in algos:
		if a == algo:
			mapping[a] = 'SELECTED'
		else:
			mapping[a] = ''
	
	for i in range(1,4):
		if i == int(dist):
			mapping['d%d' % i] = 'SELECTED'
		else:
			mapping['d%d' % i] = ''
	mapping['sql'] = sql
	mapping['err'] = err
	try:
		mapping['png'] = 'static/' + png.split('/')[-1]
	except:
		mapping['png'] = ''
	# can't have variable called one-pass
	mapping['one'] = mapping['one-pass']
	return mapping

class WebRevj:
    def index(self, sql=DEFSQL, algo=DEFALGO, dist='1'):
		if (DEFSQL == sql) and (DEFALGO == algo) and ('1' == dist):
			return tmpl.safe_substitute(
				getMapping(sql, algo, dist, DEF_PNG, ''))
		err = ''
		dot = ''		
		try:
			dot = revj.query2Dot(sql, algo)
		except:
			err = """<p>Please report bugs at this 
			<a href="https://sourceforge.net/forum/?group_id=235680">forum</a>, 
			or this <a href="https://sourceforge.net/tracker/?group_id=235680&atid=1097495">
			bugtracker</a></p>
			"""
			err += getException()
			
		#this is a fileHandle !!
		dotFile = tempfile.mkstemp('.dot', '', STATICDIR)
		os.write(dotFile[0], dot)
		os.close(dotFile[0])
		
		pngFile = tempfile.mkstemp('.png', '', STATICDIR)
		os.close(pngFile[0])
		
		
		dotRunner('one', dist, dotFile[1], pngFile[1])
		os.remove(dotFile[1])
		
		return tmpl.safe_substitute(
			getMapping(sql, algo, dist, pngFile[1], err))
       
    index.exposed = True


if __name__ == '__main__':
	revj.initGlobalGrammar()
	try:		
		f = open('form.tmpl')
		tmpl = f.read()
		f.close()
	except:
		tmpl = 'invalid template file'
	tmpl = Template(tmpl)

	cherrypy.quickstart(WebRevj(), '/', 'cherry.conf')
