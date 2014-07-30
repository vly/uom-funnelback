#!flask/bin/python

'''
Neo4j inbound links lookup API
'''

from flask import Flask, jsonify, abort, request, make_response, url_for, send_file
from flask.views import MethodView
from flask.ext.restful import Api, Resource, reqparse, fields, marshal

from py2neo import neo4j, rel
import logging
from time import sleep

from werkzeug.contrib.fixers import ProxyFix
 
app = Flask(__name__,)# static_directory= "/Users/vly/Dropbox/dev/unimelb_funnelback/linkchecker/dist")
api = Api(app)

link_fields = {
    'hostname': fields.String,
    'path': fields.String,
    'anchor': fields.String
}

page_fields = {
    'url': fields.String
}

class neo4j_db(object):
    """Connect to neo4j database"""
    def __init__(self, dbpath='http://localhost:7474/db/data'):
        self.dbpath = dbpath

    def _db_init(self):
        logging.debug("Trying to connect to db.")
        try:
            self.db = neo4j.GraphDatabaseService(self.dbpath)
            self.write_batch = neo4j.WriteBatch(self.db)
            return True
        except Exception as e:
            logging.debug("db_init: %s" % e)
            return False

    def connect(self):
        i = 0
        while True:
            a = self._db_init()
            if a:
                break
            if i == 5:
                logging.error("db_connect: DB connection failed.")
                sys.exit(1)
            i += 1
            sleep(2)

    def get_links(self, url):
        node_type = 'Pages'
        if '.pdf' in url or '.doc' in url or '.ppt' in url:
            node_type = 'Documents'

        query = neo4j.CypherQuery(self.db, 'MATCH (dom:Hostnames)-[:CONTAINS]-(a:Pages)-[k:LINKS_TO]->(b:%s) WHERE b.url="%s" AND NOT (dom.hostname = "%s") RETURN dom,a,b,k.anchor LIMIT 300' % (node_type, url, url.split('/',1)) )
        temp = []
        for x in query.stream():
            a = x[1].get_properties()
            if {"hostname": a['hostname'],"path": a['path']} not in temp:
              temp.append({"hostname": a['hostname'],"path": a['path'], "anchor": x[3]})
        return temp

    def get_similarpages(self, url):
        url = url[:-1]
        node_type = 'Pages'
        if '.pdf' in url or '.doc' in url or '.ppt' in url:
            node_type = 'Documents'
        query = neo4j.CypherQuery(self.db, 'MATCH (x:Pages)-[r:LINKS_TO]-(y:%s) WHERE y.url=~"^%s.*" AND x.url=~"^%s.*" RETURN y, count(r) ORDER BY count(r) DESC LIMIT 50' % (node_type, url,url.split('/',1)[0]) )
        temp = []
        for x in query.stream():
            a = x[0].get_properties()
            temp.append({"url": a['url']})
        return temp
 
class LinksAPI(Resource):
    # decorators = [auth.login_required]
    
    def __init__(self):
        self.db = neo4j_db()
        self.db.connect()
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('hostname', type=str)
        super(LinksAPI, self).__init__()

    def get(self):
        if 'q' not in request.args:
            abort(404)
        data = self.db.get_links(request.args['q'])
        print('here', data, request.args['q'])
        length = len(data)
        if length == 0:
            return {"data" : [], "status": "sorry, no results found.", "size": length}

        return {"data" : [marshal(x, link_fields) for x in data ], "status": "success", "size": length}

class SimilarpagesAPI(Resource):
    # decorators = [auth.login_required]
    
    def __init__(self):
        self.db = neo4j_db()
        self.db.connect()
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('hostname', type=str)
        super(SimilarpagesAPI, self).__init__()

    def get(self):
        if 'q' not in request.args:
            abort(404)
        data = self.db.get_similarpages(request.args['q'])
        print('here', data, request.args['q'])
        length = len(data)
        if length == 0:
            return {"data" : [], "status": "sorry, no results found.", "size": length}

        return {"data" : [marshal(x, page_fields) for x in data ], "status": "success", "size": length}
        

api.add_resource(LinksAPI, '/webstructure/api/v1.0/links', endpoint = 'links')
api.add_resource(SimilarpagesAPI, '/webstructure/api/v1.0/similarpages', endpoint = 'similarpages')


app.wsgi_app = ProxyFix(app.wsgi_app)

if __name__ == '__main__':
    app.run(debug = True)