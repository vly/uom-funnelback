from py2neo import neo4j, rel
import logging
from http.server import BaseHTTPRequestHandler
from http.server import HTTPServer

from urllib.parse import parse_qs
from urllib.parse import urlparse

class Linkserver(object):
	def __init__(self):
		dbpath = 'http://localhost:7474/db/data'
		self.db = neo4j.GraphDatabaseService(dbpath)

	def get_links(self, url):
		query = neo4j.CypherQuery(self.db, 'MATCH (dom:Hostnames)-[:CONTAINS]-(a:Pages)-[:LINKS_TO]->(b:Pages) WHERE b.url=~"%s." AND NOT (dom.hostname = "%s") RETURN dom,a,b LIMIT 100' % (url,url.split('/',1)) )
		temp = {}
		for x in query.stream():
			a = x[1].get_properties()
			if a['hostname'] not in temp:
				temp[a['hostname']] = set()
			temp[a['hostname']].add(a['path'])
		return temp

class RequestHandler(BaseHTTPRequestHandler):

	def do_GET(self):
		
		query = urlparse(self.path).query
		if 'q=' in query:
			query = query.split('q=')[1]
			if 'unimelb.edu.au' in query:
				temp = Linkserver()
				results = temp.get_links(query)
				if len(results) > 1:
					self.send_response(200)
					self.send_header("Content-type", "text/html")
					self.end_headers()
					for each in results:
						self.wfile.write(bytes('<h3>%s</h3>' % each, 'utf8'))
						for a in results[each]:
							self.wfile.write(bytes('<p>%s</p>' % a, 'utf8'))
 


if __name__ == '__main__':

	server = HTTPServer(('0.0.0.0', 4441), RequestHandler)
	server.serve_forever()