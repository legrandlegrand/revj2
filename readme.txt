Installation
============
Install Graphviz (tested with ver 2.20)
Install Python (tested with ver 2.4, 2.5 and 2.6)
Install Pyparsing (tested with ver 1.4.5)



Self-tests
==========
python revj.py

Note that some tests may fail in order to document bugs/future work. They are documented as "known bug"


Running on Unix
===============
gvpr minds about the line endings in the dir.g file. 


GUI
===
There is a very simple interface that displays a welcome message if everything was installed correctly. If not, check that Graphviz is installed, ex: dot is in the PATH, and if there is pyparsing.py in PYTHONPATH.

Paste the SQL in the upper text entry, select the diagram generation algorith (or leave the default "fdp"), and press "Generate diagram". If edges are cluttered, increase the "distance" parameter. In case there are any problems due to the dir.g script, please use the "one-pass" algorithm, or use the command line. 

The generated diagram is in the file "current.gif". You can also edit "current.dot" and run "dot" on top of it

To run the tool:

python gui.py



Bugs and Limitations
====================
Please qualify columns with aliases. Instead of "select a from table" use "select t.a from table t". This is trivial to fix for one table, however in general it does require access to the database. Instead of "Select name, id, dep_name from person inner join department..." use ""Select p.name, p.id, d.dep_name from person p inner join department d .."

Subselects are not fully working at this moment. For subselects use "one-pass"


Generating a diagram from standard input
========================================
Command line parameter '-' will cause text to be read from standard input.
Iy you paste directly text, end input with ^D (^Z on Windows)

python revj.py -



Generating a diagram from file
==============================
Specify filename on commandline:

python revj.py sample.sql | dot -Grankdir=LR -Edir=none -T png -o current.png



Getting nicer output from Graphviz
==================================
Emden R. Gansner suggested to generate a graph, parse it and generate a new, directed graph which looks much better

python revj.py - | fdp | gvpr -fdir.g | dot -Grankdir=LR -Edir=none -T png -o current.png


You might want to try several layout algorithms, since the graph layout varies wildly. Usually neato and fdp looks the best and are reliable.

python revj.py vesel.sql | neato | .....
python revj.py vesel.sql | circo | ...
python revj.py vesel.sql | dot | ...
python revj.py vesel.sql | fdp | ...
python revj.py vesel.sql | twopi | ...


Credits:
========
Emden R. Gansner and his colleagues at AT&T Research
Paul McGuire for developing PyParsing and code reviews
Pascal Lemoy for submitting lots of SQL fragments with bugs
