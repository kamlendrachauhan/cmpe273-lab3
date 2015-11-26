package main
import (
    "fmt"
    "hash/crc32"
    "math"
    "sort"
    "net/http"
    "io/ioutil"
    "encoding/json"
    "github.com/julienschmidt/httprouter"

)
var(
    hostURLs = []string{"http://localhost:3000","http://localhost:3001","http://localhost:3002"}
)

type keyValMap struct{
    key string
    value string
}

type Response struct{
    Key string `json:"key"`
    Value string `json:"value"`
}

type HashkeyvalSort []uint32

func (h HashkeyvalSort) Len() int{
    return len(h)
}
func (h HashkeyvalSort) Swap(i, j int){
    h[i], h[j] = h[j], h[i]
}
func (h HashkeyvalSort) Less(i, j int) bool {
    return h[i] < h[j]
}

type ConsistentHashCircle struct{
    circle map[uint32]string
    orderedKeys []uint32
    hosts []string
    weights map[string]int
}

//creating circle of hosts present to load balance
func NewCircle(hosts []string) *ConsistentHashCircle {
    consistenthashcircle := &ConsistentHashCircle{make(map[uint32]string), make([]uint32, 0), hosts, make(map[string]int),}
    consistenthashcircle.createCircle()
    return consistenthashcircle
}

//Create consistently hashed circle
func (h *ConsistentHashCircle) createCircle() {
    totalWeight := 0
    for _, node := range h.hosts {
        if weight, ok := h.weights[node]; ok {
            totalWeight += weight
        } else {
            totalWeight += 1
        }
    }

    for _, node := range h.hosts {
        weight := 1

        if _, ok := h.weights[node]; ok {
            weight = h.weights[node]
        }

        factor := math.Floor(float64(40*len(h.hosts)*weight) / float64(totalWeight))

        for j := 0; j < int(factor); j++ {
            nodeKey := fmt.Sprintf("%s-%d", node, j)
            bKey := hashGenerator(nodeKey)

            for i := 0; i < 3; i++ {
                h.circle[bKey] = node
                h.orderedKeys = append(h.orderedKeys, bKey)
            }
        }
    }

    sort.Sort(HashkeyvalSort(h.orderedKeys))
}

func hashGenerator(key string) uint32 {
    if len(key) < 64 {
        var scratch [64]byte
        copy(scratch[:], key)
        return crc32.ChecksumIEEE(scratch[:len(key)])
    }
    return crc32.ChecksumIEEE([]byte(key))
}

func (h *ConsistentHashCircle) GetNode(stringKey string) (node string, ok bool) {
    pos, ok := h.GetNodePos(stringKey)
    if !ok {
        return "", false
    }
    return h.circle[h.orderedKeys[pos]], true
}

func (h *ConsistentHashCircle) GetNodePos(stringKey string) (pos int, ok bool) {
    if len(h.circle) == 0 {
        return 0, false
    }
    key := hashGenerator(stringKey)
    nodes := h.orderedKeys
    pos = sort.Search(len(nodes), func(i int) bool { return nodes[i] > key })
    if pos == len(nodes) {
        return 1, true
    } else {
        return pos, true
    }
}

func putKeyInCache(key string, value string){
    var serverURL string
    circle := NewCircle(hostURLs)
    host,ok := circle.GetNode(key)
    if ok{
        serverURL = host +"/keys/"+key+"/"+value
    }
    fmt.Println(serverURL)
    req, err := http.NewRequest("PUT", serverURL, nil)
    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        panic(err)
    }
    resp.Body.Close()
}

func getKeyFromCache(key string)(Response, bool, error){
    var serverURL string
    var response Response
    circle := NewCircle(hostURLs)
    host,ok := circle.GetNode(key)
    if ok {
        serverURL =host+"/keys/"+key
    }
    fmt.Println(serverURL)
    resp, err := http.Get(serverURL)
    if err != nil || resp.StatusCode >= 400 {
        return Response{}, false, err
    }
    defer resp.Body.Close()

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return Response{}, false, err
    }
    err = json.Unmarshal(body, &response)
    if err != nil {
        return Response{}, false, err
    }
    return response, true, nil
}

func putKey(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
    key_id :=  p.ByName("key_id")
    value :=  p.ByName("value")

    putKeyInCache(key_id,value)

    rw.WriteHeader(200)
}
func getKey(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {

    var response Response
    key_id :=  p.ByName("key_id")

    response,ok,_ := getKeyFromCache(key_id)

    // Write content-type, statuscode, payload
    if!ok{
        rw.Header().Set("Content-Type", "application/json")
        rw.WriteHeader(500)
        return
    }
    rw.Header().Set("Content-Type", "application/json")
    jresponse,_ := json.Marshal(response)
    rw.WriteHeader(200)
    fmt.Fprintf(rw, "%s", jresponse)
}
func main(){
    mux := httprouter.New()
    mux.GET("/keys/:key_id", getKey)
    mux.PUT("/keys/:key_id/:value", putKey)
    client := http.Server{
        Addr:"0.0.0.0:5000",
        Handler: mux,
    }
    client.ListenAndServe()
}