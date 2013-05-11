#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
	Python script for converting a CSV to SQLite3 database.
	
	Copyright (C) 2013 Greger Stolt Nilsen

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import sqlite3
import sys, getopt

class Dataset:
	def __init__(self):
		self.fields = []
		self.data = []

	def add_data(self, d):
		""" Add a tuple of data """
		if len(d) != len(self.fields):
			print "Wrong number of data elements"
			return

		self.data.append(d)

	def add_fields(self, f):
		self.fields = f
		print self.fields

	def parseFile(self, filename, separator=';'):
		with open(filename) as f:
			content = f.readlines()

		# Assume first line contains field names
		fields = content[0].rstrip('\n\r').decode('utf-8').split(separator)

		self.add_fields(fields)

		# Parse all data lines
		for di in range(1, len(content)):
			data = content[di].rstrip('\n\r').decode('utf-8').split(separator)
			self.add_data(data)

	def outputDB(self, filename, tablename="dataset"):
		conn = sqlite3.connect(filename)
		c = conn.cursor()

		fieldstring = ""
		for field in self.fields:
			fieldstring += "%s text, " % field

		fieldstring = fieldstring.rstrip(', ')

		crstring = "CREATE TABLE %s (%s)" % (tablename, fieldstring)

		c.execute(crstring)

		insertstring = "INSERT INTO " + tablename + " VALUES (?"+(len(self.fields)-1)*",?" + ")"
		print insertstring
		c.executemany(insertstring, self.data)

		conn.commit()
		conn.close()

def main(argv):
	inputfile = ''
	outputfile = ''

	try:
		opts, args = getopt.getopt(argv, "hi:o:", ['ifile=', 'ofile='])
	except getopt.GetoptError:
		print 'cvs2sqlite.py -i <inputfile> -o <outputfile>'
		sys.exit(2)

	for opt, arg in opts:
		if opt == '-h':
			print 'cvs2sqlite.py -i <inputfile> -o <outputfile>'
			sys.exit()
		elif opt in ("-i", "--ifile"):
			inputfile = arg
		elif opt in ("-o", "--ofile"):
			outputfile = arg

	print "Input: ", inputfile
	print "Output: ", outputfile
	
	ds = Dataset()

	ds.parseFile(inputfile)
	ds.outputDB(outputfile)


if __name__ == '__main__':
	main(sys.argv[1:])
