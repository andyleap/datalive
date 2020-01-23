package main

import (
	"log"
	"net/http"

	"github.com/andyleap/datalive"
	"github.com/andyleap/simpleapi"
)

func New(dl *datalive.DataLive) *simpleapi.API {
	api := simpleapi.New()

	api.Route("{0}/data/{1}").Method("GET").To(dl.Get)
	api.Route("{0}/data/{1}").Body(2).Method("PUT", "POST").To(dl.Set)
	api.Route("{0}/data/{1}").Method("DELETE").To(dl.Delete)
	api.Route("{0}/data/{1}/watch").Method("GET").To(dl.Watch)

	api.Route("{0}/index/{1}/query").Body(2).Method("POST").To(dl.Index)
	api.Route("{0}/index/{1}").Body(2).Method("PUT", "POST").To(dl.CreateIndex)
	api.Route("{0}/index/{1}").Method("DELETE").To(dl.DeleteIndex)
	api.Route("{0}/index/{1}/watch").Body(2).Method("POST").To(dl.WatchIndex)

	return api
}

func main() {
	dl, err := datalive.Open("server.db")
	if err != nil {
		log.Fatal(err)
	}

	api := New(dl)

	http.Handle("/", api)
	http.ListenAndServe(":8080", nil)
}
