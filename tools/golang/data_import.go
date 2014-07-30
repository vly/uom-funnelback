package main

import (
	"bufio"
	"fmt"
	"github.com/jmcvetta/neoism"
	"log"
	"os"
	"strings"
)

type Database struct {
	db       *neoism.Database
	qb       *QueryBatch
	endpoint string
}

type PersonSummary struct {
	Name string `json:"x.name"`
}

type QueryBatch struct {
	Queries []*neoism.CypherQuery
	Size    int
}

func (db *Database) Connect(hostname string) (database *neoism.Database, ok bool) {
	hostname = hostname + "/db/data/"
	database, err := neoism.Connect(hostname)
	if err != nil {
		log.Fatal(err)
	}
	db.db = database
	ok = true
	return
}

func (db *Database) Batch(query *neoism.CypherQuery) {
	if db.qb.Size > 500 {
		db.db.CypherBatch(db.qb.Queries)
		db.qb.Queries = []*neoism.CypherQuery{query}
		db.qb.Size = 1
	} else {
		db.qb.Queries = append(db.qb.Queries, query)
		db.qb.Size += 1
	}
}

func (db *Database) BatchPurge() {
	db.db.CypherBatch(db.qb.Queries)
	db.qb.Queries = make([]*neoism.CypherQuery, 1)
	db.qb.Size = 0
}

// lookup of a person's colleagues
func (db *Database) CreateHostnames(query string) (results interface{}, err error) {
	cq := neoism.CypherQuery{
		Statement:  "CREATE (x:Site {hostname: {hostname}})",
		Parameters: neoism.Props{"hostname": query},
		Result:     &[]PersonSummary{},
	}
	db.db.Cypher(&cq)
	return
}

func (db *Database) CreatePage(hostname string, page string) {
	cq := neoism.CypherQuery{
		Statement:  "MATCH (x:Site) WHERE x.hostname={hostname} CREATE x-[:CONTAINS]->(y:Page {path: {page}})",
		Parameters: neoism.Props{"hostname": hostname, "page": page},
		Result:     &[]PersonSummary{},
	}
	db.Batch(&cq)
	return
}

// """MERGE (domain1:Subdomains {hostname: {from_domain}})-[:CONTAINS]->(page1:Pages {url: {from_uri}}))-[:LINKS_TO]->(page2:Pages {url: {to_url}})<-[:CONTAINS]-(domain2:Subdomains {hostname: {to_domain}})RETURN page1""")

func (db *Database) ProcessLine(context *map[string][]string, line []byte) {
	if line[0] == byte('+') {
		workingString := string(line[1:])
		if workingString[:4] == "http" {
			workingString = strings.SplitAfterN(workingString, "://", 2)[1]

		}
		tmp := strings.SplitN(workingString, "/", 2)
		fmt.Println(tmp)
		if _, ok := (*context)[tmp[0]]; !ok {
			(*context)[tmp[0]] = []string{"/" + tmp[1]}
			db.CreateHostnames(tmp[0])
		} else {
			(*context)[tmp[0]] = append((*context)[tmp[0]], "/"+tmp[1])
		}
		db.CreatePage(tmp[0], "/"+tmp[1])
	}

	// db.Create(query)
}

func main() {
	if f, err := os.Open("../2014-07-01/index.anchors"); err == nil {
		defer f.Close()

		db := new(Database)
		db.Connect("http://localhost:7474")
		db.qb = new(QueryBatch)

		context := make(map[string][]string, 1)
		r := bufio.NewReader(f)
		for {
			line, isPrefix, err := r.ReadLine()
			if err != nil {
				break
			}
			buf := append([]byte(nil), line...)
			for isPrefix && err == nil {
				line, isPrefix, err = r.ReadLine()
				buf = append(buf, line...)
			}

			db.ProcessLine(&context, buf)
		}
		db.BatchPurge()
	}
}
