''' Import data into Neo4j db'''

from py2neo import neo4j, cypher
import sys

try:
	import __pypy__
except ImportError:
	__pypy__ = None

the_list = ["le.unimelb.edu.au","airport.unimelb.edu.au","esrc.unimelb.edu.au","library.unimelb.edu.au","digitisation.unimelb.edu.au","lib.unimelb.edu.au","sport.unimelb.edu.au","msl.unimelb.edu.au","academichonesty.unimelb.edu.au","learningandteaching.unimelb.edu.au","equity.unimelb.edu.au","housing.unimelb.edu.au","studentadvising.unimelb.edu.au","services.unimelb.edu.au","careers.unimelb.edu.au","enrolment.unimelb.edu.au","fee.acs.unimelb.edu.au","sis.unimelb.edu.au","courseworks.unimelb.edu.au","graduation.unimelb.edu.au","nextsteps.unimelb.edu.au","studentconnect.unimelb.edu.au","safercommunity.unimelb.edu.au","elc.unimelb.edu.au","servicecommitment.unimelb.edu.au","voice.unimelb.edu.au","musse.unimelb.edu.au","upclose.unimelb.edu.au","visions.unimelb.edu.au","vcblog.unimelb.edu.au"]

db = neo4j.GraphDatabaseService('http://localhost:7474/db/data')
results = {}

for each in the_list:
	query = 'START n=node:Subdomains("hostname:*%s*") MATCH n-[r]-m WHERE r.uri=~ ".*pdf$" return count(*)' % each
	data, metadata = cypher.execute(db, query)
	results[each] = {'pdf' : data[0]}
	query = 'START n=node:Subdomains("hostname:*%s*") MATCH n-[r]-m return count(*) ORDER BY count(*) desc' % each
	data, metadata = cypher.execute(db, query)
	results[each]['all'] = data[0]

for each in results: 
	print(each,",",results[each]['all'][0], ",", results[each]['pdf'][0])