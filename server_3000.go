package main

import (
    "fmt"
    "net/http"
    "encoding/json"
    "github.com/julienschmidt/httprouter"
)

var keyValStore map[string]string

type getResponse struct {
    Key string `json:"key"`
    Value string `json:"value"`
}

type error struct {
    Error_message string `json:"error_message"`
}
func getKey(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {

    var response []byte
    key_id :=  p.ByName("key_id")
    if value,found := keyValStore[key_id]; found {
        jsonResponse := &getResponse{
            key_id,
            value,
        }
        resp,_ := json.Marshal(jsonResponse)
        response = resp
        rw.WriteHeader(200)

    }else {
        jsonResponse := &error{
            "Key does not found in the cache",
        }
        resp,_ := json.Marshal(jsonResponse)
        response = resp
        rw.WriteHeader(404)
    }

    // Write content-type, statuscode, payload
    rw.Header().Set("Content-Type", "application/json")
    fmt.Fprintf(rw, "%s", response)
}

func getKeys(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
    var output[] getResponse
    for key,val := range keyValStore{
        jsonResponse := getResponse{
            Key :key,
            Value:val,
        }
        output = append(output, jsonResponse)
    }

    resp,_ := json.Marshal(output)

    rw.Header().Set("Content-Type", "application/json")
    rw.WriteHeader(200)
    fmt.Fprintf(rw, "%s", resp)
}
func putKey(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
    key_id :=  p.ByName("key_id")
    value :=  p.ByName("value")

    keyValStore[key_id] = value

    rw.WriteHeader(200)
}
func main() {
        mux2 := httprouter.New()
        keyValStore = make(map[string]string)
        mux2.GET("/keys/:key_id", getKey)
        mux2.GET("/keys", getKeys)
        mux2.PUT("/keys/:key_id/:value", putKey)
        server2 := http.Server{
            Addr:"0.0.0.0:3000",
            Handler: mux2,
        }
        server2.ListenAndServe()
}
