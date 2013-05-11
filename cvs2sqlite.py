#!/usr/bin/python
# -*- coding: utf-8 -*-

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

		fields = content[0].rstrip('\n\r').decode('utf-8').split(separator)

		self.add_fields(fields)

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
