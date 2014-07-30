from py2neo import neo4j, rel
import sys
from time import sleep

class uploader():
    """
    Function for uploading crawl data into Neo4j
    """
    def __init__(self, dbpath, data, report_date):
        self.report_date = report_date
        self.data = data
        self.temp_subdomain = ''
        self.temp_rships = {}
        self.batch_limit = 10

        self.db_connect(dbpath)

    def debug(message):
        print(message)

    def db_init(self, dbpath):
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
            print("db_init: %s" % e)
            return False

    def batch_flush(self, batch):
        output = batch.submit()
        self.debug("batch_flush: flushing batch")
        return output

    def db_connect(self, dbpath):
        i = 0
        while True:
            a = self.db_init(dbpath)
            if a:
                break
            if i == 5:
                sys.exit(1)
            i += 1
            sleep(3)
    
    def process_data(self):
        for each in self.data:
            print("Processing:", each)
            for page in self.data[each]:
                for link in self.data[each][page]:
                    self.add_link(each + page, link)

    def process_url(self, url):
        protocol = ''
        hostname = ''
        page = ''
        temp = url
        if 'http' in url:
            i = url.split('//')[0]
            (protocol,temp) = i[:1]
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
            return output
        return

    def batch_path(self, batch, items):
        batch.get_or_create_path(items)
        return self.batch_common(batch)

    def batch_write(self, batch, index, datatype, key, value, node):
        batch.get_or_create_in_index(datatype, index, key, value, node)
        return self.batch_common(batch)

    def batch_read(self, batch, index, key, value):
        batch.get_indexed_nodes(index, key, value)
        return self.batch_common(batch)

    def add_hostname(self, hostname):
        output = self.batch_write(self.write_batch_nodes, self.db_subdomains, neo4j.Node, "hostname", hostname, Node.abstract(**{"hostname" : hostname,
            "added": self.report_date, 
            "last_seen" : self.report_date, 
            "status" : "live"}))
        return output

    def add_page(self, hostname, url):
        self.batch_write(self.write_batch_nodes, self.db_pages, neo4j.Node, "url", hostname + url, Node.abstract(**{"url" : url,
            "hostname" : hostname,
            "added": self.report_date, 
            "last_seen" : self.report_date, 
            "status" : "live"}))

    def add_link(self, from_url, to_urls):
        from_url = self.process_url(from_url)
        # to_url = self.process_url(to_url)

        # get the left sides
        left_sides = []
        for i in range(len(to_urls)):
            out = self.batch_path(self.write_batch,  {
                "OWNS" : {"hostname" : from_url["hostname"], "added" : self.report_date, "last_seen" : self.report_date, "status" : "live"},
                "CONTAINS" : {"url" : from_url["url"], "added" : self.report_date},
                "LINKS_TO" : None})
            if out:
                left_sides += [x for x in out]
        if len(self.write_batch):
            left_sides += [x for x in self.batch_flush(self.write_batch)]

        # get the right sides
        out = []
        right_sides = []
        for each in to_urls:
            temp = self.process_url(each)
            if temp:
                out = self.batch_path(self.write_batch, {"OWNS" : {"hostname" : temp["hostname"],
                                                         "added" : self.report_date,
                                                         "last_seen" : self.report_date,
                                                         "status" : "live"},
                                                "CONTAINS" : {"url" : temp["url"],
                                                              "added" : self.report_date}})
                if out:
                    right_sides += [x for x in out]
        if len(self.write_batch):
            right_sides += [x for x in self.batch_flush(self.write_batch)]

        # join the sides
        if len(left_sides) != len(right_sides):
            print("Side lengths are different.")
            # sys.exit(1)

        for each in range(len(right_sides)):
            temp = neo4j.Path.join(left_sides[each], "LINKS_TO", right_sides[each])
            # self.batch_path(self.write_batch, None, temp)



if __name__ == '__main__':
    dbpath = 'http://localhost:7474/db/data'
    report_date = '2014-08-01'

    def load_data(filename, limit=0):
        i=0
        index_limit = 10
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
    print(data)
    print("Initialising loader.")
    loader = uploader(dbpath, data, report_date)
    print("Importing data.")
    loader.process_data()