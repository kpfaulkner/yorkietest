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
	"github.com/yorkie-team/yorkie/pkg/document/json"
	"github.com/yorkie-team/yorkie/pkg/document/key"
)

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
	// create clients.
	return &yt
}

/*
func (yt *YorkieTest) watchDocs() {

	watchCh, err := cli.Watch(ctx, doc)
	if err != nil {
		fmt.Printf("watch error %v", err)
		return
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

	fmt.Printf("XXXX waiting for changes\n")
	for up := range watchCh {

		//fmt.Printf("update from watch %+v\n", up)
		m.Lock()
		err := cli.Sync(ctx, up.Keys...)
		//fmt.Printf("after sync %+v\n", up)
		if err != nil {
			fmt.Printf("sync error %v", err)
			m.Unlock()
			return
		}
		m.Unlock()
		//fmt.Printf("doc is now %+v\n", doc.Marshal())
		count++
		if count%100 == 0 {
			fmt.Printf("doc %d\n", count)
		}
	}

	done <- true

	fmt.Printf("left watch\n")
}
*/

// bulkUpdate sends a bunch of updates to all the documents (randomly)
func (yt *YorkieTest) bulkUpdate(maxUpdates int, sleepMS int) {

	for i := 0; i < maxUpdates; i++ {

		k := i % 100
		v := rand.Intn(1000)

		// 20 keys... 1000 possible values.
		key := fmt.Sprintf("k%d", k)
		value := fmt.Sprintf("v%d", v)

		// pick a random doc.
		docName := fmt.Sprintf("doc%d", rand.Intn(yt.maxDocs))
		doc := yt.allDocs[docName]
		//fmt.Printf("updating %s to %s\n", key, value)
		yt.updateDoc(doc, key, value)
		if i%100 == 0 {
			fmt.Printf("update %d\n", i)
		}
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)
	}
}

func (yt *YorkieTest) updateDoc(doc *document.Document, key string, value string) error {

	yt.docLock.Lock()
	defer yt.docLock.Unlock()

	err := doc.Update(func(root *json.Object) error {
		// value is empty so remove key.
		if value == "" {
			root.Delete(key)
		} else {
			root.SetString(key, value)
		}
		//fmt.Printf("MARSHAL %v\n", root.Marshal())

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

func (yt *YorkieTest) doBulk(msSleep int) {

	//go yt.watchDoc(doc)

	// create docs.
	for i := 0; i < yt.maxDocs; i++ {
		_, err := yt.createAndAttachDoc(fmt.Sprintf("doc%d", i))
		if err != nil {
			fmt.Printf("Cannot create doc%d : %s\n", i, err.Error())
			panic("BOOM")
		}
	}
	yt.bulkUpdate(10000, msSleep)
}

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
	cli, err := client.Dial(yt.ip+":"+yt.clientPort, client.WithAPIKey(yt.projectPublicKey))
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

func (yt *YorkieTest) createProject(projectName string) error {
	project, err := yt.aCli.CreateProject(yt.ctx, projectName)
	if err != nil {
		fmt.Printf("create project err %s\n", err.Error())
		return err
	}
	fmt.Printf("created project %+v\n", *project)
	return nil
}

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

	//doSync := flag.Bool("sync", false, "do sync instead of update")
	//listenOnly := flag.Bool("listen", false, "only listen for changes")
	//text := flag.String("text", "default message", "text")
	msSleep := flag.Int("sleep", 100, "ms sleep between updates")
	doBulkUpdate := flag.Bool("dobulk", false, "do bulk update")
	createProject := flag.Bool("createproject", false, "create project")
	projectName := flag.String("project", "", "project name")
	projectApiKey := flag.String("projectapi", "", "project apikey")
	keyName := flag.String("key", "", "key")
	value := flag.String("value", "", "value")
	docName := flag.String("doc", "", "doc key")
	maxDocs := flag.Int("maxdocs", 20, "max docs to generate when doing bulk updates")
	readDoc := flag.Bool("read", false, "read doc")
	listDoc := flag.Bool("list", false, "list docs for project")
	ip := flag.String("ip", "10.0.0.39", "yorkie ip")
	clientPort := flag.String("port", "8080", "client connection port")

	flag.Parse()

	yt := NewYorkieTest(*ip, *clientPort, *projectApiKey, *maxDocs)

	//yt.adminLogin()
	yt.connectClient()

	// create project and bail
	if *createProject {
		err := yt.createProject(*projectName)
		if err != nil {
			fmt.Printf("create project error %s\n", err.Error())
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
		}
		return
	}

	/*
		// if random specified and NOT docname
		if *docRandom && *docName == "" {
			for i := 0; i < *maxDocs; i++ {
				dn := fmt.Sprintf("doc%d", i)
				doc := document.New(key.Key(dn))
				if err = cli.Attach(ctx, doc); err != nil {
					fmt.Printf("new doc error %v\n", err)
					return
				}
				allDocs[dn] = doc
			}
		} */

	if *readDoc {
		//yt.watchDoc(allDocs)
		//fmt.Printf(doc.Marshal())
		return
	}

	if *doBulkUpdate {
		yt.doBulk(*msSleep)
	} else {
		yt.updateDoc(doc, *keyName, *value)
		err = yt.cli.Detach(yt.ctx, doc)
		if err != nil {
			fmt.Printf("unable to detach : %s\n", err.Error())
		}
	}

}

func merge(cs ...<-chan int) <-chan int {
	out := make(chan int)
	var wg sync.WaitGroup
	wg.Add(len(cs))
	for _, c := range cs {
		go func(c <-chan int) {
			for v := range c {
				out <- v
			}
			wg.Done()
		}(c)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
