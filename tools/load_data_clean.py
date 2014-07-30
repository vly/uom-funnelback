from py2neo import neo4j, rel
import sys
from time import sleep

class uploader():
    """
    Function for uploading crawl data into Neo4j
    """
    def __init__(self, dbpath, data, report_date, debug_on=0):
        self.report_date = report_date
        self.data = data
        self.temp_subdomain = ''
        self.temp_rships = {}
        self.batch_limit = 10
        self.debug_on = debug_on

        self.db_connect(dbpath)

    def db_init(self, dbpath):
        self.debug("db_init: Trying to connect to db.")
        try:
            self.db = neo4j.GraphDatabaseService(dbpath)
            self.db_subdomains = self.db.get_or_create_index(neo4j.Node, "Subdomains")
            self.db_pages = self.db.get_or_create_index(neo4j.Node, "Pages")
            self.db_affinity = self.db.get_or_create_index(neo4j.Relationship, "Pageowners")
            self.db_links = self.db.get_or_create_index(neo4j.Relationship, "Links")

            self.read_batch = neo4j.ReadBatch(self.db)
            self.write_batch = neo4j.WriteBatch(self.db)
            self.write_batch_nodes = neo4j.WriteBatch(self.db)
            return True
        except Exception as e:
            self.debug("db_init: %s" % e)
            return False

    def debug(self, message):
        if self.debug_on:
            print(message)

    def db_connect(self, dbpath):
        i = 0
        while True:
            a = self.db_init(dbpath)
            if a:
                break
            if i == 5:
                self.debug("db_connect: DB connection failed.")
                sys.exit(1)
            i += 1
            sleep(3)
    
    def process_data(self):
        self.start_node, = self.db.create({"domain": "unimelb.edu.au"})
        for each in self.data:
            self.debug("processing_data: " + each)
            for page in self.data[each]:
                for link in self.data[each][page]:
                    if len(each+page) > 5 and len(link) > 5: 
                        self.add_link(each + page, link)

    def process_url(self, url):
        protocol = ''
        hostname = ''
        page = ''
        temp = url
        if 'http' in url and len(url) > 7:
            i = url.split('//')[0]
            (protocol,temp) = i[:1]
        if len(temp.split('/',1)) == 2:
            (hostname, page) = temp.split('/',1)
            page = '/' + page

            if 'unimelb.edu.au' not in hostname:
                return

            return {"hostname" : hostname,
                    "url" : page,
                    "protocol" : protocol}

    def batch_common(self, batch):
        if len(batch) == self.batch_limit:
            output = batch.submit()
            self.debug("batch_common: flushing batch")
            return output
        return

    def batch_flush(self, batch):
        output = batch.submit()
        self.debug("batch_flush: flushing batch")
        return output

    def batch_path(self, batch, *items):
        batch.get_or_create_path(*items)
        return self.batch_common(batch)

    def add_link(self, from_url, to_urls):
        from_url = self.process_url(from_url)
        self.debug("add_link: left side %s" % from_url)
        # to_url = self.process_url(to_url)
        for each in to_urls:
            temp = self.process_url(each)
            print(temp)
            query = neo4j.CypherQuery(self.db,
                    """MERGE (domain1:Subdomains {hostname: {from_domain}})-[:CONTAINS]->(page1:Pages {url: {from_uri}}))-[:LINKS_TO]->(page2:Pages {url: {to_url}})<-[:CONTAINS]-(domain2:Subdomains {hostname: {to_domain}})
                        RETURN page1""")
            result = query.execute( from_domain = from_url['hostname'], from_uri = from_url['url'], to_domain = temp['hostname'], to_url = temp['url'])
            print([x for x in result])



if __name__ == '__main__':

    dbpath = 'http://localhost:7474/db/data'
    report_date = '2013-08-01'

    def load_data(filename, limit=0):
        i=0
        index_limit = 200
        data = {}
        k = 1

        with open(filename, 'r') as f:
            while k:
                try:
                    k = f.readline()
                    i += 1
                    if k:
                        if k[0] == '+':
                            point_domain = k[1:].split('/')[0]
                            point_page = '/' + k[1:-1].split('/', 1)[1]
                            if 'www.' in point_domain:
                                point_domain = point_domain[4:]
                            if point_domain not in data:
                                data[point_domain] = {}
                            if point_page not in data[point_domain]:
                                data[point_domain][point_page] = []
                        else:
                            blob = k.split(' --- ')
                            if len(blob) > 1:
                                if 'www.' in blob[0]:
                                    blob[0] = blob[0][4:]
                                if ']' in blob[1]:
                                    blob[1] = blob[1].split(']')[1][:-1]
                                if 'thumb' in blob[1] or 'images' in blob[1][:6]:
                                    blob[1] = '<image>'    
                                data[point_domain][point_page].append(blob)
                except UnicodeDecodeError:
                    pass
                if limit:
                    if len(data) == limit:
                        break
        return data

    print("Loading data.")
    data = load_data("2014-07-01/index.anchors", 5)
    print("Initialising loader.")
    loader = uploader(dbpath, data, report_date, 1)
    print("Importing data.")
    loader.process_data()