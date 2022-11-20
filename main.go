package main

import (
	"context"
	json2 "encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yorkie-team/yorkie/admin"
	"github.com/yorkie-team/yorkie/client"
	"github.com/yorkie-team/yorkie/pkg/document"
	"github.com/yorkie-team/yorkie/pkg/document/json"
	"github.com/yorkie-team/yorkie/pkg/document/key"
)

const (

	// just some test JSON data.
	valueTemplate = `{
      id: "test-id-%d",
      visibility: true,
      more: {
        type: "testingstuff",
        details: {
          type: "xxxx",
          values: [%d, %d],
        },
      },
    }`
)

type updateDetails struct {
	key   string
	value string
}

// YorkieTest used to test out the Yorkie server. Can write/read documents, subscribe to changes, do bulk updates etc.
type YorkieTest struct {

	// max number of docs we're dealing with.
	maxDocs int

	// all docs we're attached to.
	allDocs map[string]*document.Document

	// client to Yorkie
	cli *client.Client

	// admin client to Yorkie
	aCli *admin.Client

	ctx context.Context

	// ugly lock used when calling Sync against Yorkie. Will look at a nicer option later, but for now
	// working well.
	docLock sync.Mutex

	// yorkie ip
	ip         string
	clientPort string

	// only deal with single project at a time.
	projectPublicKey string
}

func NewYorkieTest(ip string, clientPort string, projectPublicKey string, maxDocs int) *YorkieTest {
	yt := YorkieTest{}
	yt.docLock = sync.Mutex{}
	yt.ctx = context.Background()
	yt.ip = ip
	yt.clientPort = clientPort
	yt.projectPublicKey = projectPublicKey
	yt.maxDocs = maxDocs

	yt.allDocs = make(map[string]*document.Document)
	return &yt
}

// watchDocs will subscribe to changes to all docs contained in the allDocs map.
func (yt *YorkieTest) watchDocs() {

	var allChannels []<-chan client.WatchResponse
	for _, doc := range yt.allDocs {
		yt.docLock.Lock()
		watchCh, err := yt.cli.Watch(yt.ctx, doc)
		yt.docLock.Unlock()
		if err != nil {
			fmt.Printf("watch error %v", err)
			panic("BOOM")
		}
		allChannels = append(allChannels, watchCh)
	}

	docUpdateChannel := mergeChannelWatch(allChannels...)
	count := 0
	oldCount := 0
	ticker := time.NewTicker(1 * time.Second)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				fmt.Printf("%d per second\n", count-oldCount)
				oldCount = count
			}
		}
	}()

	for up := range docUpdateChannel {

		yt.docLock.Lock()
		err := yt.cli.Sync(yt.ctx, up.Keys...)
		fmt.Printf("sync.. type %v :  keys %+v\n", up.Type, up.Keys)
		if err != nil {
			fmt.Printf("sync error %v", err)
			yt.docLock.Unlock()
			return
		}
		yt.docLock.Unlock()
		count++
		if count%100 == 0 {
			fmt.Printf("doc %d\n", count)
		}
	}
	done <- true
}

// bulkUpdate sends a bunch of updates to all the documents (randomly)
// sleeps sleepMS milliseconds between each update.
func (yt *YorkieTest) bulkUpdate(maxUpdates int, sleepMS int) {

	t := time.Now()
	for i := 0; i < maxUpdates; i++ {

		// maximum dealing with 100 docs...  will make configurable later. TODO(kpfaulkner)
		k := i % 100
		v := rand.Intn(1000)

		key := fmt.Sprintf("k%d", k)
		value := fmt.Sprintf(valueTemplate, v, rand.Intn(200), rand.Intn(200))

		// pick a random doc.
		docName := fmt.Sprintf("doc%d", rand.Intn(yt.maxDocs))
		doc := yt.allDocs[docName]

		//fmt.Printf("updating doc %s : key %s to %s\n", docName, key, value)
		err := yt.updateDoc(doc, key, value, rand.Intn(20))
		if err != nil {
			fmt.Printf("bulkUpdate updateDoc err %s\n", err.Error())

		}

		if i%100 == 0 {
			fmt.Printf("update %d\n", i)
		}
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)
		fmt.Printf("Took %d ms\n", time.Since(t).Milliseconds())
		t = time.Now()
	}
}

// updateDoc updates a single doc with a single key/value pair.
func (yt *YorkieTest) updateDoc(doc *document.Document, key string, value string, pos int) error {

	yt.docLock.Lock()
	defer yt.docLock.Unlock()
	err := doc.Update(func(root *json.Object) error {

		// value is empty so remove key.
		if value == "" {
			root.Delete(key)
		} else {
			root.SetString(key, value)
		}
		return nil
	}, "updates doc "+doc.Key())
	if err != nil {
		fmt.Printf("update doc error %v", err)
		return err
	}

	err = yt.cli.Sync(yt.ctx, doc.Key())
	if err != nil {
		fmt.Printf("sync error %v", err)
		return err
	}

	return nil
}

// doBulk performs bulk update against all the docs.
// Creates the docs first (0-> maxDocs) then starts the watching and updating process.
func (yt *YorkieTest) doBulk(msSleep int) {

	// create docs.
	for i := 0; i < yt.maxDocs; i++ {
		_, err := yt.createAndAttachDoc(fmt.Sprintf("doc%d", i))
		if err != nil {
			fmt.Printf("Cannot create doc%d : %s\n", i, err.Error())
			panic("BOOM")
		}
	}

	// watch all docs for updates.
	go yt.watchDocs()

	// Just do 10000 updates for now. Will make configurable TODO(kpfaulkner)
	yt.bulkUpdate(10000, msSleep)
}

// adminLogin creates an admin client. Used for making projects.
func (yt *YorkieTest) adminLogin() error {

	addr := fmt.Sprintf("%s:11103", yt.ip)
	aCli, err := admin.Dial(addr)
	if err != nil {
		fmt.Printf("admin err %s\n", err.Error())
		return err
	}

	_, err = aCli.LogIn(yt.ctx, "admin", "admin")
	if err != nil {
		fmt.Printf("unable to log in %s\n", err.Error())
		return err
	}
	yt.aCli = aCli
	return nil
}

// connectClient login and activate client
func (yt *YorkieTest) connectClient(certFile string) error {

	// AU cert ./cert2.pem
	// US cert ./cert-us.pem
	cli, err := connectClient(yt.ctx, yt.ip+":"+yt.clientPort, yt.projectPublicKey, certFile)
	if err != nil {
		fmt.Printf("dial error %v", err)
		return err
	}
	yt.cli = cli
	return nil
}

// createProject will create a project of a given name.
func (yt *YorkieTest) createProject(projectName string) error {
	project, err := yt.aCli.CreateProject(yt.ctx, projectName)
	if err != nil {
		fmt.Printf("create project err %s\n", err.Error())
		return err
	}

	fmt.Printf("created project %+v\n", *project)
	return nil
}

// createProject will create a project of a given name.
func (yt *YorkieTest) listProjects() error {

	projects, err := yt.aCli.ListProjects(yt.ctx)
	if err != nil {
		fmt.Printf("list projects err %s\n", err.Error())
		return err
	}

	for _, p := range projects {
		fmt.Printf("Project Name %s : Public key %s\n", p.Name, p.PublicKey)
	}
	return nil
}

// listDocumentsForProject list document keys for a given project.
func (yt *YorkieTest) listDocumentsForProject(projectName string) error {
	docs, err := yt.aCli.ListDocuments(yt.ctx, projectName)

	if err != nil {
		fmt.Printf("cannot list docs for project %s : %s\n", projectName, err.Error())
		return err
	}

	for i, d := range docs {
		fmt.Printf("Doc %d : %s\n", i, d.Key.String())
	}

	return nil
}

// createAndAttachDoc creates a document and attaches it to the client. Also adds to the allDocs map.
func (yt *YorkieTest) createAndAttachDoc(docId string) (*document.Document, error) {

	doc, err := createAndAttachDoc(yt.ctx, yt.cli, docId)
	if err != nil {
		fmt.Printf("create doc error %v", err)
		return nil, err
	}
	yt.allDocs[docId] = doc
	return doc, nil
}

func (yt *YorkieTest) importFile(importFileName string, doc *document.Document) error {
	data, _ := os.ReadFile(importFileName)
	err := doc.Update(func(root *json.Object) error {

		// clear the root object
		members := root.Members()
		for k, _ := range members {
			root.Delete(k)
		}

		makeYorkieDoc(data, doc.Root())

		return nil
	}, "updates doc "+doc.Key())
	if err != nil {
		log.Fatalf("Error updating doc: %v\n", err)
	}

	err = yt.cli.Sync(yt.ctx, doc.Key())
	if err != nil {
		fmt.Printf("sync error %v", err)
		return err
	}

	return nil
}

func (yt *YorkieTest) readDoc(docName string) (*document.Document, error) {
	doc, err := yt.createAndAttachDoc(docName)
	if err != nil {
		fmt.Printf("create doc error %s\n", err.Error())
		return nil, err
	}

	yt.docLock.Lock()
	err = yt.cli.Sync(yt.ctx, doc.Key())
	yt.docLock.Unlock()

	if err != nil {
		fmt.Printf("unable to read doc : %v", err)
		return nil, err
	}

	// just output to stdout for now... will return string later.
	fmt.Printf("doc %s\n", doc.Marshal())

	return doc, nil
}

// watchDoc loops and reads a doc..
func (yt *YorkieTest) watchDoc(docName string) error {
	doc, err := yt.createAndAttachDoc(docName)
	if err != nil {
		log.Printf("create doc error %s\n", err.Error())
		return err
	}

	watchCh, err := yt.cli.Watch(yt.ctx, doc)
	if err != nil {
		log.Printf("watch error %s\n", err.Error())
		return err
	}

	count := 0
	oldCount := 0
	ticker := time.NewTicker(1 * time.Second)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				fmt.Printf("%d per second\n", count-oldCount)
				oldCount = count
			}
		}
	}()

	// loop until channel closes...
	for change := range watchCh {
		fmt.Printf("change %+v\n", change)
		yt.cli.Sync(yt.ctx, doc.Key())
		//fmt.Printf("doc %+v\n", doc.Marshal())
		count++
	}

	return nil
}

func main() {
	fmt.Printf("So it begins...\n")

	//defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	//defer profile.Start(profile.MemProfile, profile.MemProfileRate(1), profile.ProfilePath(".")).Stop()

	msSleep := flag.Int("sleep", 100, "ms sleep between updates")
	doBulkUpdate := flag.Bool("dobulk", false, "do bulk update")
	createProject := flag.Bool("createproject", false, "create project")
	projectName := flag.String("project", "", "project name")
	projectApiKey := flag.String("projectapi", "", "project apikey")
	keyName := flag.String("key", "", "key")
	value := flag.String("value", "", "value")
	docName := flag.String("doc", "", "doc key")
	importFile := flag.String("import", "", "file to import document from")
	certFile := flag.String("cert", "", "file path to cert")
	maxDocs := flag.Int("maxdocs", 20, "max docs to generate when doing bulk updates")
	concurrent := flag.Int("concurrent", 1, "number of concurrent clients to use")
	watchDoc := flag.Bool("watch", false, "watch doc")
	readDoc := flag.Bool("read", false, "read doc")
	listDoc := flag.Bool("list", false, "list docs for project")
	listProjects := flag.Bool("listprojects", false, "list projects")
	ip := flag.String("ip", "10.0.0.39", "yorkie ip")
	clientPort := flag.String("port", "8080", "client connection port")

	flag.Parse()

	start := time.Now()
	defer func() {
		fmt.Printf("total time %d seconds\n", int(time.Since(start).Seconds()))
	}()

	yt := NewYorkieTest(*ip, *clientPort, *projectApiKey, *maxDocs)

	yt.adminLogin()
	yt.connectClient(*certFile)

	// create project and bail
	if *createProject {
		err := yt.createProject(*projectName)
		if err != nil {
			fmt.Printf("create project error %s\n", err.Error())
		}
		return
	}

	// list projects
	if *listProjects {
		err := yt.listProjects()
		if err != nil {
			fmt.Printf("list project error %s\n", err.Error())
		}
		return
	}

	// just read a doc.
	if *watchDoc {
		yt.watchDoc(*docName)
		return
	}

	if *readDoc {
		_, err := yt.readDoc(*docName)
		if err != nil {
			log.Printf("read doc error %s", err.Error())
		}
		return
	}

	if *listDoc {
		err := yt.listDocumentsForProject(*projectName)
		if err != nil {
			fmt.Printf("admin err %s\n", err.Error())
		}
		return
	}

	var doc *document.Document
	var err error
	// if specify doc name... just do the one.
	if *docName != "" {
		doc, err = yt.createAndAttachDoc(*docName)
		if err != nil {
			fmt.Printf("createAndAttachDoc err %s\n", err.Error())
			return
		}
	}

	// import a file to be written to doc.
	if *importFile != "" {
		err := yt.importFile(*importFile, doc)
		if err != nil {
			log.Printf("importFile err %s", err.Error())
		}
		return
	}

	if *doBulkUpdate {
		yt.doBulk(*msSleep)
	} else {

		// if we have key/value... then do that.
		// otherwise loop and make up a heap of random content.
		if *keyName != "" && *value != "" {
			yt.updateDoc(doc, *keyName, *value, 1)
			err = yt.cli.Detach(yt.ctx, doc)
			if err != nil {
				fmt.Printf("unable to detach : %s\n", err.Error())
			}
			return
		}

		doConcurrentUpdates(*concurrent, fmt.Sprintf("%s:%s", yt.ip, yt.clientPort), *projectApiKey, *certFile, *docName, 100000, *msSleep)

		return
	}

}

func makeYorkieDoc(data []byte, root *json.Object) error {

	var stuff map[string]interface{}
	err := json2.Unmarshal(data, &stuff)
	if err != nil {
		log.Fatalf("Error unmarshalling: %v\n", err)
	}
	populateObject(root, stuff)
	return nil
}

func populateArray(doc *json.Array, a []interface{}) {
	for _, v := range a {
		switch v.(type) {
		case bool:
			doc.AddBool(v.(bool))
		case []byte:
			doc.AddBytes(v.([]byte))
		case int64:
			doc.AddLong(v.(int64))
		case time.Time:
			doc.AddDate(v.(time.Time))
		case int:
			doc.AddInteger(v.(int))
		case float32, float64:
			doc.AddDouble(v.(float64))
		case string:
			doc.AddString(v.(string))
		case map[string]interface{}:
			log.Fatalf("BOOM have object in array... unsure this is allowed!!!\n")
		case []interface{}:
			newArray := doc.AddNewArray()
			populateArray(newArray, v.([]interface{}))
		default:
			fmt.Printf("I don't know about type %T!\n", v)
		}
	}
}

func populateObject(doc *json.Object, m map[string]interface{}) {
	for k, v := range m {
		switch v.(type) {
		case bool:
			doc.SetBool(k, v.(bool))
		case []byte:
			doc.SetBytes(k, v.([]byte))
		case int64:
			doc.SetLong(k, v.(int64))
		case time.Time:
			doc.SetDate(k, v.(time.Time))
		case int:
			doc.SetInteger(k, v.(int))
		case float32, float64:
			doc.SetDouble(k, v.(float64))
		case string:
			doc.SetString(k, v.(string))
		case map[string]interface{}:
			newMap := doc.SetNewObject(k)
			populateObject(newMap, v.(map[string]interface{}))
		case []interface{}:
			newArray := doc.SetNewArray(k)
			populateArray(newArray, v.([]interface{}))
		default:
			fmt.Printf("I don't know about type %T!\n", v)
		}
	}
}

// mergeChannelWatch just allows to watch all channels at once for document updates.
func mergeChannelWatch(cs ...<-chan client.WatchResponse) <-chan client.WatchResponse {
	overallCh := make(chan client.WatchResponse, 100000)
	var wg sync.WaitGroup
	wg.Add(len(cs))
	for _, ch := range cs {
		go func(ch <-chan client.WatchResponse) {
			for v := range ch {
				overallCh <- v
			}
			wg.Done()
		}(ch)
	}
	go func() {
		wg.Wait()
		close(overallCh)
	}()
	return overallCh
}

func connectClient(ctx context.Context, addr string, projectKey string, certFileLocation string) (*client.Client, error) {

	options := []client.Option{
		client.WithAPIKey(projectKey), client.WithMaxRecvMsgSize(5000000),
	}

	if certFileLocation != "" {
		options = append(options, client.WithCertFile(certFileLocation))
	}

	cli, err := client.Dial(addr, options...)

	if err != nil {
		fmt.Printf("dial error %v", err)
		return nil, err
	}

	err = cli.Activate(ctx)
	if err != nil {
		fmt.Printf("activate error %v", err)
		return nil, err
	}

	return cli, nil
}

func createAndAttachDoc(ctx context.Context, cli *client.Client, docId string) (*document.Document, error) {
	doc := document.New(key.Key(docId))
	if err := cli.Attach(ctx, doc); err != nil {
		fmt.Printf("new doc error %v\n", err)
		return nil, err
	}
	return doc, nil
}

// completely separate from rest of functions.
// Create new documents and attach them for each goroutine.
// Want to check what impact locks have on it.
func doConcurrentUpdates(noCurrentUpdates int, addr string, projectKey string, certFile string, docId string, noUpdates int, sleepInMs int) {

	updateChannel := make(chan updateDetails, noUpdates)

	for i := 0; i < noUpdates; i++ {
		k := fmt.Sprintf("key%d", rand.Intn(100))
		v := fmt.Sprintf(valueTemplate, i, rand.Intn(200), rand.Intn(200))
		updateChannel <- updateDetails{k, v}
	}

	var counter atomic.Int32
	wg := sync.WaitGroup{}
	ctx := context.Background()
	for i := 0; i < noCurrentUpdates; i++ {
		cli, err := connectClient(ctx, addr, projectKey, certFile)
		if err != nil {
			log.Fatalf("unable to connect to client : %v", err)
		}

		doc, err := createAndAttachDoc(ctx, cli, docId)
		if err != nil {
			log.Fatalf("unable to connect to client : %v", err)
		}

		// local copy... just so loop and go routines dont screw up.
		cli2 := cli
		doc2 := doc
		wg.Add(1)
		go func(cli *client.Client, doc *document.Document) {
			for update := range updateChannel {
				counter.Add(1)
				updateDoc(ctx, cli, doc, update.key, update.value)

				if counter.Load()%100 == 0 {
					fmt.Printf("Completed %d updates\n", counter.Load())
				}

				time.Sleep(time.Duration(sleepInMs) * time.Millisecond)
			}
			wg.Done()
			fmt.Printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX done\n")
		}(cli2, doc2)
	}

	wg.Wait()

}

func updateDoc(ctx context.Context, cli *client.Client, doc *document.Document, key string, value string) error {

	err := doc.Update(func(root *json.Object) error {

		// value is empty so remove key.
		if value == "" {
			root.Delete(key)
		} else {
			root.SetString(key, value)
		}

		return nil
	}, "updates doc "+doc.Key())
	if err != nil {
		fmt.Printf("update doc error %v", err)
		return err
	}

	err = cli.Sync(ctx, doc.Key())
	if err != nil {
		fmt.Printf("sync error %v", err)
		return err
	}

	return nil
}
