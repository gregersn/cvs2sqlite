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
		#print self.fields

	def parseFile(self, filename, header=True):
		with open(filename, 'rb') as f:
			# Sniff out the format of the csv file
			dialect = csv.Sniffer().sniff(f.read(2048))
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

	def outputDB(self, filename, tablename="dataset"):
		# Connect to database
		conn = sqlite3.connect(filename)

		# Get database cursor
		c = conn.cursor()

		# Generate a string containing all column/field names for use in SQL
		fieldstring = ""
		for field in self.fields:
			fieldstring += "%s text, " % field
		fieldstring = fieldstring.rstrip(', ')
	
		# Create table from fieldstring		
		crstring = "CREATE TABLE %s (%s)" % (tablename, fieldstring)
		c.execute(crstring)

		# Create INSERT string
		insertstring = "INSERT INTO " + tablename + " VALUES (?"+(len(self.fields)-1)*",?" + ")"

		# Add all data to database
		c.executemany(insertstring, self.data)

		conn.commit()
		conn.close()

def main(argv):
	# Default values
	inputfile = 'infile'
	outputfile = 'outfile'
	tablename = 'dataset'
	header = True

	usage = 'cvs2sqlite.py -i <inputfile> -o <outputfile> -t <tablename> --noheader'

	# Parse command line options
	try:
		opts, args = getopt.getopt(argv, "hi:o:t:", ['ifile=', 'ofile=', 'noheader', 'table='])
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
		else:
			print opt

	
	print "Input: ", inputfile
	print "Output: ", outputfile
	print "Table name: ", tablename
	
	ds = Dataset()
	ds.parseFile(inputfile, header)
	ds.outputDB(outputfile, tablename)


if __name__ == '__main__':
	main(sys.argv[1:])
