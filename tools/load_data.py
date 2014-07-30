from py2neo import neo4j, rel
import pickle

try:
    self.db = neo4j.GraphDatabaseService('http://vly.unimelb.edu.au:7474/db/data')
    self.db_subdomains = db.get_or_create_index(neo4j.Node, "Subdomains")
    self.db_pagelist = db.get_or_create_index(neo4j.Node, "Pages")
    self.db_pages = db.get_or_create_index(neo4j.Relationship, "Pageowners")
    self.db_links = db.get_or_create_index(neo4j.Relationship, "Links")
    
except:
    print('Failed to connect')
    

report_date = '2013-05-17'



class Cruncher():
    def __init__(self):
        self.data = ''
        self.temp_subdomain = ''
        self.temp_rships = {}
        self.index_limit = 10
        self.batch_limit = 10
        self.write_batch_rels_counter = 0
        self.write_batch_nodes_counter = 0
        self.read_batch = neo4j.ReadBatch(db)
        self.write_batch_rels = neo4j.WriteBatch(db)
        self.write_batch_nodes = neo4j.WriteBatch(db)
    
    def load_data(self, filename):
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
        return data

    def check_ambiguity(self, results):
        '''
        Check for multiple returning results
        '''
        if len(results) > 1:
            print("Ambiguous search results, check for duplicates")
            return results
        if not results:
            return
        return results[0]
    
    def add_to_index(self, index, node, ikey):
        self.write_batch_rels.add_to_index(neo4j.Node, index, "hostname", ikey, node)
        self.write_batch_nodes_counter += 1
        if self.write_batch_nodes_counter == self.batch_limit:
            self.write_batch_nodes.submit()
            self.write_batch_nodes_counter = 0
    
    def create_subdomain_node(self, hostname):
        temp = db.create({"hostname" : hostname})
        self.add_to_index(db_subdomains, temp[0], hostname)
        return temp[0]
    
    def get_subdomain_node(self, hostname, search_type='fuzzy'):
        '''
        Searches for hostname and checks if multiple results are returned
        '''
        if self.temp_subdomain and self.temp_subdomain['hostname'] == hostname:
            return self.temp_subdomain
        if search_type == 'exact':
            query = 'hostname:' + hostname + '*'
        else:
            query = 'hostname:*' + hostname + '*'
            
        if hostname == 'unimelb.edu.au':
            query = 'hostname:' + hostname
        else:
            query = query.split('unimelb.edu.au')[0] + '*'
        results = [x for x in db_subdomains.query(query)]
        if not results:
            return self.create_subdomain_node(hostname)
        else:
            if len(results) > 1 and search_type != 'exact':
                return self.get_subdomain_node(hostname, 'exact')
        return self.check_ambiguity(results)
    
    def get_subdomain_page_node(self, hostname, page):
        '''
        Return connecting page node for a matching URI relationship
        '''
        temp = self.get_subdomain_node(hostname)
        if not temp:
            print('No results returned')
            return
        print(temp)
        self.temp_subdomain = temp
        if not len(self.temp_rships) or hostname not in self.temp_rships:
            if len(self.temp_rships) == self.index_limit:
                self.temp_rships.popitem()
            self.temp_rships[hostname] = db.match(start_node=temp, rel_type='CONTAINS')
            
        results = self.check_ambiguity([z for z in self.temp_rships[hostname] if page == z['uri']])
        if not results:
            print("No page found")
            new_node = self.create_subdomain_page_node(page)
            return new_node
        if type(results) == list:
            return results[0].end_node
        return results.end_node
    
    def get_subdomain_link(self, url):
        protocol = ''
        hostname = ''
        page = ''
        temp = url
        if 'http' in url:
            i = url.split('//')[0]
            (protocol,temp) = i[:1]
        (hostname, page) = temp.split('/',1)
        page = '/' + page
        print(hostname, page)
        if 'unimelb.edu.au' not in hostname:
            print('Non-unimelb link:',hostname, page)
            return
        return self.get_subdomain_page_node(hostname, page)
    
    def create_subdomain_page_node(self, page):
        print("page:", page)
        temp = db_pagelist.get_or_create("url", page, {'url': page, 'added': report_date})
        #self.batch_write_add(self.temp_subdomain, temp, "contains")
        self.db_pages.get_or_create('CONTAINS', self.temp_subdomain['hostname'] + page, (self.temp_subdomain, "contains", temp))
        return temp
        
    def batch_write_add(self, start_node, end_node, relation="LINKS_TO", page="general"):
        self.write_batch_rels.create_in_index(neo4j.Relationship, db_links, "link", page, rel(start_node, relation, end_node))
        self.write_batch_rels_counter += 1
        if self.write_batch_rels_counter == 10:
            self.write_batch_flush()
            
    def write_batch_flush(self):
        self.write_batch_rels.submit()
        self.write_batch_rels_counter = 0


if __name__ == '__main__':
    self.dbpath = 'http://vly.unimelb.edu.au:7474/db/data'

    data = load_data()
    # output = open('data.pkl', 'wb')

    # Pickle dictionary using protocol 0.
    # pickle.dump(data, output)

    # pkl_file = open('data.pkl', 'rb')
    # data = pickle.load(pkl_file)

    blin = Cruncher()

for a in test.data:
    print("Entering", a)
    for b in test.data[a]:
        working_node = test.get_subdomain_page_node(a, b)
        print(test.data[a])
        for each in test.data[a][b]:
            page_node = test.get_subdomain_link(each[0])
            print(type(working_node), type(page_node))
            if page_node:
                test.batch_write_add(working_node, page_node, "LINKS_TO")
test.write_batch_flush()


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

    def db_init(self, dbpath):
        try:
            self.db = neo4j.GraphDatabaseService(dbpath)
            self.db_subdomains = db.get_or_create_index(neo4j.Node, "Subdomains")
            self.db_pages = db.get_or_create_index(neo4j.Node, "Pages")
            self.db_affinity = db.get_or_create_index(neo4j.Relationship, "Pageowners")
            self.db_links = db.get_or_create_index(neo4j.Relationship, "Links")

            self.read_batch = neo4j.ReadBatch(db)
            self.write_batch = neo4j.WriteBatch(db)
            self.write_batch_nodes = neo4j.WriteBatch(db)
            return True
        except:
            return False

    def db_connect(self, dbpath):
        i = 0
        while not self.db_init(dbpath):
            if i == 5:
                sys.exit(1)
            i += 1
    
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
            out = self.batch_path(self.write_batch, None, ["OWNS" : {"hostname" : from_url["hostname"],
                                                     "added" : self.report_date,
                                                     "last_seen" : self.report_date,
                                                     "status" : "live"},
                                            "CONTAINS" : {"url" : from_url["url"],
                                                          "added" : self.report_date},
                                            "LINKS_TO" : None])
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
                out = self.batch_path(self.write_batch, None, ["OWNS" : {"hostname" : temp["hostname"],
                                                         "added" : self.report_date,
                                                         "last_seen" : self.report_date,
                                                         "status" : "live"},
                                                "CONTAINS" : {"url" : temp["url"],
                                                              "added" : self.report_date}])
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
    self.dbpath = 'http://vly.unimelb.edu.au:7474/db/data'

    
test.write_batch_flush()