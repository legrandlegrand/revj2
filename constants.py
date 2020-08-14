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

import os, platform

DEFSQL = """SELECT 
 film.film_id AS FID, 
 film.title AS title, 
 film.description AS description, 
 category.name AS category, 
 film.rental_rate AS price, 
 film.length AS length, 
 film.rating AS rating, 
 GROUP_CONCAT(CONCAT(actor.first_name, _utf8' ', actor.last_name) SEPARATOR ', ') AS actors 
FROM 
 category LEFT JOIN 
 film_category ON 
  category.category_id = film_category.category_id 
 LEFT JOIN film ON 
  film_category.film_id = film.film_id 
 JOIN film_actor ON 
  film.film_id = film_actor.film_id 
 JOIN actor ON 
  film_actor.actor_id = actor.actor_id 
GROUP BY film.film_id; """
	
DEFALGO = 'neato'
	
DOTFILE = 'current.dot'
GIFFILE = 'current.gif'
DIRGUNIX = 'dir.g'
DIRGDOS = 'dir_dos.g'

DEF_PNG = 'sakila.png'

WELLCOME = 'Hi there! Reverse Snowflake Join works correctly' 

OUTERJOINCOLOR = 'orange'
OUTERJOINARROW = 'dot'

CLUSTERCOLORS = ['lightgray', 'antiquewhite', 'aquamarine', 'cadetblue', 'coral',
	'darkorange']


#Edit this path !! Keep in sync with the one in cherry.conf!!
STATICDIR = os.getcwd() + '/static'

try:
	os.mkdir(STATICDIR)
except:
	pass

"""this func runs once. chooses file with correct line ending for gvpr"""
x=platform.win32_ver()
globals()['DIRG'] = DIRGUNIX
if x[0] != '':
	globals()['DIRG'] = DIRGDOS
