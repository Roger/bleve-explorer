package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"io/ioutil"
	"net/http"
	bleveHttp "github.com/blevesearch/bleve/http"
)

type varLookupFunc func(req *http.Request) string

type DocsIndexHandler struct {
	defaultIndexName string
	IndexNameLookup  varLookupFunc
}

func NewDocsIndexHandler(defaultIndexName string) *DocsIndexHandler {
	return &DocsIndexHandler{
		defaultIndexName: defaultIndexName,
	}
}

func (h *DocsIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	// find the index to operate on
	var indexName string
	if h.IndexNameLookup != nil {
		indexName = h.IndexNameLookup(req)
	}
	if indexName == "" {
		indexName = h.defaultIndexName
	}
	index := bleveHttp.IndexByName(indexName)
	if index == nil {
		showError(w, req, fmt.Sprintf("no such index '%s'", indexName), 404)
		return
	}

	batch := index.NewBatch()

	// read the request body
	requestBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		showError(w, req, fmt.Sprintf("error reading request body: %v", err), 400)
		return
	}

	// parse request body as json
	var docs []interface{}
	err = json.Unmarshal(requestBody, &docs)
	if err != nil {
		showError(w, req, fmt.Sprintf("error parsing request body as JSON: %v", err), 400)
		return
	}

	for id, doc := range docs {
		docId := strconv.Itoa(id)
		err = batch.Index(docId, doc)

		if (id+1)%1000 == 0 {
			fmt.Printf("Indexing batch (%d docs)...\n", id+1)
			err := index.Batch(batch)
			if err != nil {
				showError(w, req, fmt.Sprintf("error indexing document: %d: %v", id, err), 500)
			}
			batch = index.NewBatch()
		}
		if err != nil {
			showError(w, req, fmt.Sprintf("error indexing document: %s: %v", docId, err), 500)
		}
	}


	idxErr := index.Batch(batch)
	if idxErr != nil {
		showError(w, req, fmt.Sprintf("error indexing documents %v", idxErr), 500)
	}


	rv := struct {
		Status string `json:"status"`
	}{
		Status: "ok",
	}
	mustEncode(w, rv)
}
