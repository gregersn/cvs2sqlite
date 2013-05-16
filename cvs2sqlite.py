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
import csv

def is_int(i):
	try:
		v = int(i)
		return True
	except ValueError:
		return False

def is_float(f):
	try:
		v = float(f)
		return True
	except ValueError:
		return False

class Dataset:
	def __init__(self):
		self.fields = []
		self.data = []
		self.tables = {}

	def add_table(self, table, fields, data):
		_table['fields'] = fields
		_table['data'] = data

		self.tables[table] = _table

	def add_data(self, d):
		""" Add a tuple of data """
		if len(d) != len(self.fields):
			print "Wrong number of data elements"
			print d
			return

		self.data.append(d)

	def add_fields(self, f):
		self.fields = f
		#print self.fields

	def parseFile(self, filename, header=True, sniff=2048):
		with open(filename, 'rb') as f:
			# Sniff out the format of the csv file
			dialect = csv.Sniffer().sniff(f.read(sniff))
			f.seek(0)

			# Read as sniffed
			reader = csv.reader(f, dialect)
			
			if header is True:
				# Assume field names in first row
				fields = reader.next()
				self.add_fields([f.decode('utf-8') for f in fields])

			# Add all data
			for row in reader:
				if header is False:
					self.add_fields([('Field_%s' % str(i+1)).decode('utf-8') for i in range(len(row))])
				self.add_data([f.decode('utf-8') for f in row])

	def normalize(self, threshold=5):
		""" Try to split out fields which have few distinct values into their own tables """
		fields = {}
		for f in self.fields:
			fields[f] = {}
		print fields
		for data in self.data:
			for c in range(len(data)):
				if data[c] not in fields[self.fields[c]]:
					fields[self.fields[c]][data[c]] = 1	
				else:
					fields[self.fields[c]][data[c]] += 1	
				

		# Loop through fields
		for f in fields:
			# In case there are less unique values than threshold
			if 1 < len(fields[f]) <= threshold:
				# Process field
				print "Processing field: %s, %d" % (f, len(fields[f]))
				table_name = 't_'+f
				table_fields = (f,)
				table_values = []
				for k,v in fields[f].iteritems():
					table_values.append([k,])

				self.tables[table_name] = (table_name, table_fields, table_values,)

		#print self.tables

	def createTable(self, filename, tablename, fields, data):
		# Connect to database

		#print tablename
		#print fields
		#print data
		conn = sqlite3.connect(filename)

		# Get database cursor
		c = conn.cursor()

		# Generate a string containing all column/field names for use in SQL
		fieldstring = ""
		for field in fields:
			fieldstring += "%s text, " % field
		fieldstring = fieldstring.rstrip(', ')
	
		# Create table from fieldstring		
		crstring = "CREATE TABLE %s (%s)" % (tablename, fieldstring)
		#print crstring
		c.execute(crstring)

		# Create INSERT string
		insertstring = "INSERT INTO " + tablename + " VALUES (?"+(len(fields)-1)*",?" + ")"
		#print insertstring
		# Add all data to database
		c.executemany(insertstring, data)

		conn.commit()
		conn.close()

	def outputDB(self, filename, tablename="dataset"):
		if len(self.tables) > 0:
			print "Multiple tables: "
			for table in self.tables:
				_tablename = self.tables[table][0]
				_fields = self.tables[table][1]
				_data = self.tables[table][2]
				self.createTable(filename, _tablename, _fields, _data)

		self.createTable(filename, tablename, self.fields, self.data)

def main(argv):
	# Default values
	inputfile = 'infile'
	outputfile = 'outfile'
	tablename = 'dataset'
	header = True
	normalize = False
	sniff = 2048

	usage = 'cvs2sqlite.py -i <inputfile> -o <outputfile> -t <tablename> --noheader -s <snifflength>'
	
	# Parse command line options
	try:
		opts, args = getopt.getopt(argv, "hi:o:t:s:", ['ifile=', 'ofile=', 'noheader', 'table=', 'normalize', 'sniff='])
	except getopt.GetoptError:
		print usage
		sys.exit(2)

	for opt, arg in opts:
		if opt == '-h':
			print "Help; ", usage
			sys.exit()
		elif opt in ("-i", "--ifile"):
			inputfile = arg
		elif opt in ("-o", "--ofile"):
			outputfile = arg
		elif opt in ("-t", "--table"):
			tablename = arg
		elif opt in ("-n", "--noheader"):
			header = False
		elif opt in ("--normalize"):
			normalize = True
		elif opt in ("-s", "--sniff"):
			sniff = opt
		else:
			print opt

	
	print "Input: ", inputfile
	print "Output: ", outputfile
	print "Table name: ", tablename
	
	ds = Dataset()
	ds.parseFile(inputfile, header, sniff=sniff)
	if normalize is True:
		ds.normalize()
	ds.outputDB(outputfile, tablename)


if __name__ == '__main__':
	main(sys.argv[1:])
