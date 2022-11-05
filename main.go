package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/yorkie-team/yorkie/admin"
	"github.com/yorkie-team/yorkie/client"
	"github.com/yorkie-team/yorkie/pkg/document"
	"github.com/yorkie-team/yorkie/pkg/document/crdt"
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

// updatePositioning
func (yt *YorkieTest) updatePositioning(doc *document.Document, key string, value string) error {

	yt.docLock.Lock()
	defer yt.docLock.Unlock()

	err := doc.Update(func(root *json.Object) error {
		// value is empty so remove key.
		if value == "" {
			root.Delete(key)
		} else {
			root.SetString(key, value)

			var ar *json.Array
			if !root.Has("mapEntities") {
				ar = root.SetNewArray(key)
			} else {
				ar = root.GetArray(key)
			}
			ar.AddString()

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

// updateDoc updates a single doc with a single key/value pair.
func (yt *YorkieTest) updateDoc(doc *document.Document, key string, value string, pos int) error {

	yt.docLock.Lock()
	defer yt.docLock.Unlock()

	err := doc.Update(func(root *json.Object) error {
		// value is empty so remove key.
		if value == "" {
			root.Delete(key)
		} else {

			var obj *json.Object
			if root.Has(key) {
				//fmt.Printf("get obj\n")
				obj = root.GetObject(key)
			} else {
				fmt.Printf("set obj\n")
				obj = root.SetNewObject(key)
			}

			// value of json for particular features/drawing.
			obj.SetString("value", value)

			// put position.
			obj.SetInteger("pos", pos)
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
func (yt *YorkieTest) connectClient() error {

	cli, err := client.Dial(yt.ip+":"+yt.clientPort, client.WithAPIKey(yt.projectPublicKey), client.WithMaxRecvMsgSize(5000000))
	if err != nil {
		fmt.Printf("dial error %v", err)
		return err
	}

	err = cli.Activate(yt.ctx)
	if err != nil {
		fmt.Printf("activate error %v", err)
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

	_, err := yt.aCli.GetProject(yt.ctx, "kdkdkd")
	if err != nil {
		fmt.Printf("list projects err %s\n", err.Error())
		return err
	}

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

	doc := document.New(key.Key(docId))
	if err := yt.cli.Attach(yt.ctx, doc); err != nil {
		fmt.Printf("new doc error %v\n", err)
		return nil, err
	}

	yt.allDocs[docId] = doc
	return doc, nil
}

func main() {
	fmt.Printf("So it begins...\n")

	msSleep := flag.Int("sleep", 100, "ms sleep between updates")
	doBulkUpdate := flag.Bool("dobulk", false, "do bulk update")
	createProject := flag.Bool("createproject", false, "create project")
	projectName := flag.String("project", "", "project name")
	projectApiKey := flag.String("projectapi", "", "project apikey")
	keyName := flag.String("key", "", "key")
	value := flag.String("value", "", "value")
	docName := flag.String("doc", "", "doc key")
	maxDocs := flag.Int("maxdocs", 20, "max docs to generate when doing bulk updates")
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
	yt.connectClient()

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
		doc, err := yt.createAndAttachDoc(*docName)
		if err != nil {
			fmt.Printf("create doc error %s\n", err.Error())
			return
		}

		yt.docLock.Lock()
		watchCh, err := yt.cli.Watch(yt.ctx, doc)
		yt.docLock.Unlock()
		if err != nil {
			fmt.Printf("watch error %v", err)
			panic("BOOM")
		}
		for change := range watchCh {
			fmt.Printf("change %+v\n", change)
			yt.cli.Sync(yt.ctx, doc.Key())
			fmt.Printf("doc %+v\n", doc.Marshal())

		}
	}

	if *readDoc {
		doc, err := yt.createAndAttachDoc(*docName)
		if err != nil {
			fmt.Printf("create doc error %s\n", err.Error())
			return
		}

		yt.docLock.Lock()
		err = yt.cli.Sync(yt.ctx, doc.Key())
		yt.docLock.Unlock()

		if err != nil {
			fmt.Printf("unable to read doc : %v", err)
			panic("BOOM")
		}

		fmt.Printf("doc %s\n", doc.Marshal())
		a := doc.Root().Has("entityHierarchy")
		fmt.Printf("has %s\n", a)
		eh := doc.Root().Get("entityHierarchy")
		//eh := doc.Root().GetObject("entityHierarchy")
		fmt.Printf("EH %s\n", eh.Marshal())
		ar := eh.(*crdt.Array)

		for _, arr := range ar.Elements() {
			fmt.Printf("ARR %s\n", arr.Marshal())
		}

		me := doc.Root().GetObject("mapEntities")
		fmt.Printf("ME %s\n", me.Marshal())
		f1 := me.Get("f32c369b-4c72-49c0-b0e7-740a0ae3cfbe")
		fmt.Printf("f1 %s\n", f1.Marshal())

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

		for i := 0; i < 10000; i++ {
			k := fmt.Sprintf("key%d", rand.Intn(100))
			v := fmt.Sprintf(valueTemplate, i, rand.Intn(200), rand.Intn(200))
			//t := time.Now()
			err = yt.updateDoc(doc, k, v, 1)
			if err != nil {
				fmt.Printf("unable to update doc : %s\n", err.Error())
			}

			if i%100 == 0 {
				fmt.Printf("update %d\n", i)
			}

			// sync it
			err = yt.cli.Sync(yt.ctx, doc.Key())
			if err != nil {
				fmt.Printf("sync error %v", err)
				//return
			}

			//fmt.Printf("took %d ms\n", time.Since(t).Milliseconds())
			time.Sleep(time.Duration(*msSleep) * time.Millisecond)
		}

		return
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
