''' Import data into Neo4j db'''

from py2neo import neo4j
import sys

try:
	import __pypy__
except ImportError:
	__pypy__ = None

class DataImport:
	def __init__(self, filename, date=None):
		if __pypy__:
			self.data_file = open(filename)
		else:
			self.data_file = open(filename, encoding='latin-1') # encoding req.
			
		self.db = neo4j.GraphDatabaseService('http://localhost:7474/db/data')
		self.db_subdomains = self.db.get_or_create_index(neo4j.Node, "Subdomains")
		self.db_pages = self.db.get_or_create_index(neo4j.Relationship, "Pages")

		self.report_date = date
		if not date:
			self.report_date = '2014-07-01'

		self.read_batch = neo4j.ReadBatch(self.db)
		self.write_batch = neo4j.WriteBatch(self.db)

		self.counter = 0
		self.temp_pagelist = [0,0]

	def import_data(self):
		line = self.data_file.readline()

		# while "aapt.search.lib" not in line:
		# 	line = self.data_file.readline()
		# print('here')
		# while "aapt.search.lib" in line:
		# 	line = self.data_file.readline()
		# print('here 2')
		while line:
			block = {}
			line = self.data_file.readline()
			if '[' in line and ']' in line:
				url = line.split('] ')
				if len(url) > 1:
					url = url[1][:-1]
					if '.au' in url:
						block['hostname'] = url.split('.au')[0] + '.au'
						block['uri'] = url.split('.au')[1]
					elif '.edu/' in url:
						block['hostname'] = url.split('.edu')[0]
						block['uri'] = url.split('.edu')[1]
					if len(block) == 2 and 'theaustralian' not in block['hostname'] \
						and 'Network issues' not in block['hostname']\
						and '@' not in block['hostname']\
						and 'href' not in block['hostname']:
						self.create_node(block)
						if self.counter == 5:
							self.write_batch.submit()
							self.counter = 0

			line = self.data_file.readline()

	def get_pagelist(self, subdomain):
		rships = self.db.match(start_node=subdomain, rel_type='contains')
		temp_batch = neo4j.ReadBatch(self.db)

		i = 1
		output = []
		final_output = {}

		temp_length = len(rships)-1
		for j in range(temp_length):
			temp_batch.get_properties(rships[i])
			if i % 5 == 0 or i == temp_length:
				output += temp_batch.submit()
				temp_batch.clear()

		for each in range(len(output)-1):
			final_output[output[each]['uri']] = rships[i]
		return final_output

	def create_node(self, block):
		''' 
		create nodes
		'''
		if 'https' in block['hostname']:
			temp_subdomain = self.db_subdomains.query('hostname:*' + (block['hostname'].split('://')[1]).split('.au')[0] + '*')
		else:
			temp_subdomain = self.db_subdomains.query('hostname:' + block['hostname'].split('.au')[0] + '*')
		print('create node', temp_subdomain, block['hostname'])
		if not len(temp_subdomain):
			temp_subdomain = self.db.get_or_create_indexed_node(
					"Subdomains",
					"hostname", block['hostname'], {
						"hostname" : block['hostname'],
						"added" : self.report_date, 
						'status' : 'live',
						'last_seen' : self.report_date
					})

		else:
			temp_subdomain = temp_subdomain[0]
			self.write_batch.set_property(temp_subdomain, "last_seen", self.report_date)
			
		self.counter += 1

		#sys.exit()
		if block['hostname'] != self.temp_pagelist[0] and block['hostname']:
			self.temp_pagelist = [block['hostname'], self.get_pagelist(temp_subdomain)]

		if block['uri'] not in self.temp_pagelist[1]:
			temp_subdomain.get_or_create_path(
					("contains", {"uri" : block['uri'], "last_seen" : self.report_date, "added" : self.report_date, 'status' : 'live'})
				) 
		else:
			self.write_batch.set_property(self.temp_pagelist[1][block['uri']], "last_seen", self.report_date)


		
if __name__ == "__main__":
	importer = DataImport("2014-07-01/unimelb-report-5-metadata.txt")
	importer.import_data()