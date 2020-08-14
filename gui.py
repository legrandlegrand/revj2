"""Reverse Snowflake Joins
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

import os, platform, sys, traceback
from Tkinter import *
from revj import *

from constants import *

def deleteFiles():
	for x in [DOTFILE, GIFFILE]:
		try:
			os.remove(x)
		except:
			pass
	
def dotRunner(algo = 'one', dist = 1):
	""" PhotoImage supports only GIF format"""
	cmd = ''
	if 'one' in algo:
		#if there is some problem in the script for gvpr
		cmd = 'dot -Grankdir=LR -Granksep=%s -Edir=none -T gif -o "%s" "%s"' % \
			(dist, GIFFILE, DOTFILE)
	else:
		cmd = ('%s "%s" | gvpr -f"%s" | dot -Grankdir=LR -Granksep=%s -Edir=none '
			'-T gif -o "%s"') % (algo, DOTFILE, DIRG, dist, GIFFILE)
			
	os.system(cmd)

class GUIApp  :
	def __init__(self, master):
		pan = PanedWindow(orient=VERTICAL)
		pan.pack(fill=BOTH, expand=1)

		#resizable frame for scrollable input
		sqlFrame = PanedWindow(pan)
		pan.add(sqlFrame)
		sqlDummyFrame = Frame(sqlFrame)
		sqlDummyFrame.pack()
		sqlFrame.add(sqlDummyFrame)
		self.sqlInput = Text(sqlDummyFrame)
		self.sqlInput.insert(INSERT, DEFSQL)
		self.sqlInput.pack(expand=True, fill=BOTH, side=LEFT)
		sqlScrol = Scrollbar(sqlDummyFrame)
		sqlScrol.pack(expand=False, fill=Y, side=RIGHT)
		self.sqlInput.config(yscrollcommand=sqlScrol.set)
		sqlScrol.config(command=self.sqlInput.yview)

        #resizable frame with fixed size buttons and menus
		decor = PanedWindow(pan)
		middleDummy = Frame(decor)
		middleDummy.pack()

		label = Label(middleDummy, text="Graph algorithm=")
		label.pack(side=LEFT)

		self.varAlgo = Variable(master)
		self.varAlgo.set("neato")
		self.lstAlgo = OptionMenu(middleDummy, self.varAlgo, 
			'neato', 'fdp', 'twopi', 'circo')
			#one pass not functioning 
			#, 'one-pass (in case of trouble)')
		self.lstAlgo.pack(expand=False, side=LEFT)

		labelDist = Label(middleDummy, text="Distance=")
		labelDist.pack(side=LEFT)
		self.varDist = Variable(master)
		self.varDist.set("1")
		self.lstDist = OptionMenu(middleDummy, self.varDist, '1', '2', '3')
		self.lstDist.pack(expand=False, side=LEFT)
		
		self.btn = Button(middleDummy, text = "Generate Diagram", 
			command = self.gen)
		self.btn.pack(expand=False, side=LEFT)

		pan.add(decor)

		#resizable frame for scrollable diagram
		diagramFrame = PanedWindow(pan)
		pan.add(diagramFrame)
		diagramDummyFrame = Frame(diagramFrame)
		diagramDummyFrame.pack()
		diagramFrame.add(diagramDummyFrame)
		diagramDummyFrame2 = Frame(diagramDummyFrame)
		diagramDummyFrame2.pack(expand=True, fill=BOTH, side=TOP)
		self.canvas = Canvas(diagramDummyFrame2, 
			scrollregion=(0, 0, 2000, 2000))
		self.canvas.pack(expand=True, fill=BOTH, side=LEFT)
		#don't forget to load the image !!!!
		self.load('')
		self.gen(WELLCOME)

		diagramVScrol = Scrollbar(diagramDummyFrame2)
		diagramVScrol.pack(expand=False, fill=Y, side=RIGHT)
		self.canvas.config(yscrollcommand=diagramVScrol.set)
		diagramVScrol.config(command=self.canvas.yview)

		diagramHScrol = Scrollbar(diagramDummyFrame, orient=HORIZONTAL)
		diagramHScrol.pack(expand=False, fill=X, side=BOTTOM)
		self.canvas.config(xscrollcommand=diagramHScrol.set)
		diagramHScrol.config(command=self.canvas.xview)

	def load(self, fname=GIFFILE):
		try:
			if fname == '':
				raise Exception('except will make an empty image')
			self.graph = PhotoImage(file = fname)
		except :
			self.graph = PhotoImage()
		self.canvas.create_image(0, 0, image = self.graph, anchor = NW)


	def gen(self, wellcome = ''):
		deleteFiles()
		f = open('current.dot', 'w')
		try:
			if wellcome == '': 
				s = self.sqlInput.get(1.0, END)
				s = query2Dot(s, self.varAlgo.get())
			else:
				s = """graph  
				{	
					node [shape=record, fontsize=12];
					ERROR [label="%s" color=white];
				}""" % wellcome
		except:
			exc = sys.exc_info()[2]
			readableExc = traceback.extract_tb(exc)[-1][-1]
			s = """graph  
			{	
				node [shape=record, fontsize=12];
				ERROR [label="%s" fontcolor=red color=white];
			}""" % sys.exc_info()[1] #+ ',' + readableExc
		
		f.write(s)
		f.close()
		
		#algo moved to subGraphDotRunner()
		#a = self.varAlgo.get()
		
		s = dotRunner('one-pass', self.varDist.get())
		
		self.load()


#this is needed for revj initialization
initGlobalGrammar()

deleteFiles()

root = Tk()
app = GUIApp(root)
root.title('Reverse Snowflake Joins')
root.mainloop()