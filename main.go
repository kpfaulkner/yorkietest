package main

import (
	"context"
	json2 "encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yorkie-team/yorkie/admin"
	"github.com/yorkie-team/yorkie/client"
	"github.com/yorkie-team/yorkie/pkg/document"
	"github.com/yorkie-team/yorkie/pkg/document/json"
	"github.com/yorkie-team/yorkie/pkg/document/key"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

type updateStats struct {
	syncDurationMS  int64
	sleepDurationMS int64
}

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

	// cert file
	certFile string
}

func NewYorkieTest(ip string, clientPort string, projectPublicKey string, maxDocs int, certFile string) *YorkieTest {
	yt := YorkieTest{}
	yt.docLock = sync.Mutex{}
	yt.ctx = context.Background()
	yt.ip = ip
	yt.clientPort = clientPort
	yt.projectPublicKey = projectPublicKey
	yt.maxDocs = maxDocs
	yt.certFile = certFile
	yt.allDocs = make(map[string]*document.Document)
	return &yt
}

// watchDocs will subscribe to changes to all docs contained in the allDocs map.
// watchIdleDocs is just watching for docs we KNOW we wont update. This means we're just
// keeping the connection open.
func (yt *YorkieTest) watchDocs(noDocs int, projectPublicKey string, certFile string, createProjectPerDoc bool, syncDocs bool, watchIdleDocs bool) error {

	ctx := context.Background()
	addr := fmt.Sprintf("%s:%s", yt.ip, yt.clientPort)

	var allChannels []<-chan client.WatchResponse
	type docAndClient struct {
		doc *document.Document
		cli *client.Client
	}

	docs := make(map[string]docAndClient)

	// create a client per doc...  watching for updates.
	for i := 0; i < noDocs; i++ {

		var docName string
		if watchIdleDocs {
			docName = generateIdleName(i)
		} else {
			docName = generateBulkName(i)
		}
		fmt.Printf("connecting for doc %s\n", docName)

		var pubKey string
		if createProjectPerDoc {
			key, err := yt.createProjectReturnKey(ctx, docName)
			if err != nil {
				log.Printf("unable to create project %s : err %s", docName, err.Error())
				return err
			}
			pubKey = key
		} else {
			pubKey = projectPublicKey
		}
		cli, err := connectClient(ctx, addr, pubKey, certFile)
		if err != nil {
			log.Printf("cannot connect to yorkie : %s", err.Error())
			return err
		}

		doc, err := createAndAttachDoc(ctx, cli, docName)
		if err != nil {
			log.Printf("cannot create doc: %s", err.Error())
			return err
		}
		docs[docName] = docAndClient{doc: doc, cli: cli}
		watchCh, err := cli.Watch(ctx, doc)
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

	// purely notifying of update... but not GETTING the updated content
	// slow... since we have a single goroutine handling all changes...
	// writing isn't slow due to goroutine per client...
	for up := range docUpdateChannel {
		//fmt.Printf("%s : sync.. type %v :  keys %+v\n", time.Now().String(), up.Type, up.Keys)

		if syncDocs {
			// if we really want to measure the performance of the sync.
			for _, k := range up.Keys {

				docClient := docs[string(k)]
				start := time.Now()
				err := docClient.cli.Sync(yt.ctx, k)
				fmt.Printf("sync took %d ms\n", time.Since(start).Milliseconds())
				if err != nil {
					fmt.Printf("sync error %v", err)
					return err
				}
			}

		}

		count += len(up.Keys)
	}
	done <- true

	fmt.Printf("watch finish\n")
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

func (yt *YorkieTest) createProjectReturnKey(ctx context.Context, docName string) (string, error) {
	projectName := fmt.Sprintf("project%s", docName)
	if err := yt.createProject(projectName); err != nil {
		if status, ok := status.FromError(err); ok {
			// only check if project already exists... otherwise return a 500.
			if status.Code() != codes.AlreadyExists {
				// doesn't exist...  bomb with erro
				log.Printf("unable to create project %s : error %s", projectName, err.Error())
				return "", err
			}
		} else {
			log.Printf("unable to create project %s : error %s", projectName, err.Error())
			return "", err
		}
	}

	project, err := yt.aCli.GetProject(ctx, projectName)
	if err != nil {
		log.Printf("unable to get project %s : error %s", projectName, err.Error())
		return "", err
	}

	return project.PublicKey, nil
}

// doBulk performs bulk update against all the docs.
// generates noDocs number of documents.
// creates concurrentPerDoc clients per doc.
// createProjectPerDoc indicates if each document should have it's own
// project. Need to confirm if this impacts Yorkies performance or not.
func (yt *YorkieTest) doBulk(msSleep int, concurrentPerDoc int, noDocs int, noUpdates int, createProjectPerDoc bool) error {

	statsCh := make(chan updateStats, 1000000)

	wg := sync.WaitGroup{}

	// add WG total here.
	wg.Add(noDocs * concurrentPerDoc)
	addr := fmt.Sprintf("%s:%s", yt.ip, yt.clientPort)
	startWG := sync.WaitGroup{}
	startWG.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	var counter atomic.Int32
	for i := 0; i < noDocs; i++ {
		docName := generateBulkName(i)
		var publicKey string
		if createProjectPerDoc {
			key, err := yt.createProjectReturnKey(ctx, docName)
			if err != nil {
				log.Printf("cannot create project %s : err %s\n", docName, err.Error())
				return err
			}
			publicKey = key
		} else {
			publicKey = yt.projectPublicKey
		}

		go doConcurrentUpdates(concurrentPerDoc, addr, publicKey, yt.certFile, docName, noUpdates, msSleep, &wg, &startWG, &counter, statsCh)

		// slow down creation...
		time.Sleep(100 * time.Millisecond)
	}

	// hack
	time.Sleep(5 * time.Second)

	startWG.Done()
	fmt.Printf("waiting\n")

	// stats gathering goroutine
	go func(ctx context.Context) {
		startTime := time.Now()
		for {
			select {
			case <-time.After(1 * time.Second):
				elapsedSinceStart := time.Now().Sub(startTime)
				perSecond := float64(counter.Load()) / elapsedSinceStart.Seconds()
				fmt.Printf("average %f per second\n", perSecond)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	stopCh := make(chan struct{})
	displayStatsWG := sync.WaitGroup{}
	displayStatsWG.Add(1)
	go func(statsCh chan updateStats, stopCh chan struct{}, displayStatsWG *sync.WaitGroup) {

		minSleepDurationMS := int64(1000000)
		maxSleepDurationMS := int64(0)
		minSyncDurationMS := int64(1000000)
		maxSyncDurationMS := int64(0)
		totalSleepDurationMS := int64(0)
		totalSyncDurationMS := int64(0)
		statsCount := 0
		done := false
		for !done {
			select {
			case <-stopCh:
				done = true
			case stats := <-statsCh:

				//fmt.Printf("stats %v\n", stats)
				totalSleepDurationMS += stats.sleepDurationMS
				totalSyncDurationMS += stats.syncDurationMS
				if stats.syncDurationMS < minSyncDurationMS {
					minSyncDurationMS = stats.syncDurationMS
				}

				if stats.syncDurationMS > maxSyncDurationMS {
					maxSyncDurationMS = stats.syncDurationMS
				}

				if stats.sleepDurationMS < minSleepDurationMS {
					minSleepDurationMS = stats.sleepDurationMS
				}

				if stats.sleepDurationMS > maxSleepDurationMS {
					maxSleepDurationMS = stats.sleepDurationMS
				}
				statsCount++
			}
		}

		fmt.Printf("\n\n")
		fmt.Printf("average sleep duration 	%d ms\n", totalSleepDurationMS/int64(statsCount))
		fmt.Printf("average sync duration 	%d ms\n", totalSyncDurationMS/int64(statsCount))
		fmt.Printf("min sleep duration 	%d ms\n", minSleepDurationMS)
		fmt.Printf("max sleep duration	%d ms\n", maxSleepDurationMS)
		fmt.Printf("min sync duration 	%d ms\n", minSyncDurationMS)
		fmt.Printf("max sync duration 	%d ms\n", maxSyncDurationMS)
		displayStatsWG.Done()
	}(statsCh, stopCh, &displayStatsWG)

	wg.Wait()
	cancel()
	stopCh <- struct{}{}
	displayStatsWG.Wait()
	return nil
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
	_, err := yt.aCli.CreateProject(yt.ctx, projectName)
	if err != nil {
		//fmt.Printf("create project err %s\n", err.Error())
		return err
	}

	//fmt.Printf("created project %+v\n", *project)
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

		makeYorkieDoc(data, root)

		fmt.Printf("import data %s\n", doc.Marshal())
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

func (yt *YorkieTest) readDoc(doc *document.Document) error {

	yt.docLock.Lock()
	err := yt.cli.Sync(yt.ctx, doc.Key())
	yt.docLock.Unlock()

	if err != nil {
		fmt.Printf("unable to read doc : %v", err)
		return err
	}

	// just output to stdout for now... will return string later.
	fmt.Printf("doc %s\n", doc.Marshal())

	return nil
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
	overallCh := make(chan client.WatchResponse, 1000000)
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
// Stats returned via statsCh
func doConcurrentUpdates(noConcurrentUpdates int, addr string, projectKey string, certFile string, docId string,
	noUpdates int, sleepInMs int, wg *sync.WaitGroup, startWG *sync.WaitGroup, counter *atomic.Int32, statsCh chan updateStats) {

	updateChannel := make(chan updateDetails, noUpdates)

	for i := 0; i < noUpdates; i++ {
		k := fmt.Sprintf("key%d", rand.Intn(100))
		v := fmt.Sprintf(valueTemplate, i, rand.Intn(200), rand.Intn(200))
		updateChannel <- updateDetails{k, v}
	}

	close(updateChannel)

	ctx := context.Background()
	for i := 0; i < noConcurrentUpdates; i++ {
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
		go func(cli *client.Client, doc *document.Document, startWG *sync.WaitGroup, identifier int, statsCh chan updateStats) {
			startWG.Wait()
			for update := range updateChannel {
				start := time.Now()
				//newUpdateKey := fmt.Sprintf("%s-%d", update.key, identifier)
				newUpdateKey := update.key
				stats, err := updateDoc(ctx, cli, doc, newUpdateKey, update.value)
				if err != nil {
					log.Printf("unable to update doc : %v", err)
					continue
				}
				counter.Add(1)
				totalSleep := time.Duration(sleepInMs) * time.Millisecond
				timeTaken := time.Since(start)
				sleepDuration := totalSleep - timeTaken

				if sleepDuration > 0 {
					time.Sleep(sleepDuration)
				} else {
					log.Printf("Took too long ( %d ms), no sleeping", timeTaken.Milliseconds())
				}

				stats.sleepDurationMS = sleepDuration.Milliseconds()
				statsCh <- *stats
			}
			wg.Done()
		}(cli2, doc2, startWG, i, statsCh)
	}
}

func updateDoc(ctx context.Context, cli *client.Client, doc *document.Document, key string, value string) (*updateStats, error) {

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
		return nil, err
	}

	syncStart := time.Now()
	err = cli.Sync(ctx, doc.Key())
	if err != nil {
		fmt.Printf("sync error %v", err)
		return nil, err
	}
	syncEnd := time.Now()
	stats := updateStats{syncDurationMS: syncEnd.Sub(syncStart).Milliseconds()}
	return &stats, nil
}

func generateBulkName(i int) string {
	return fmt.Sprintf("bulkdoc%d", i)
}

func generateIdleName(i int) string {
	return fmt.Sprintf("idledoc%d", i)
}

// catchInterrupt will catch ctrl+c and exit the program with an appropriate
// report based on the command given. TODO(kpfaulkner) implement me!!
func catchInterrupt() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	fmt.Printf("interrupt..........\n")
	os.Exit(1)
}

func main() {
	fmt.Printf("So it begins...\n")

	go catchInterrupt()

	//defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	//defer profile.Start(profile.MemProfile, profile.MemProfileRate(1), profile.ProfilePath(".")).Stop()

	msSleep := flag.Int("sleep", 100, "ms sleep between updates per client connection")
	projectName := flag.String("project", "", "project name")
	commandToRun := flag.String("cmd", "", "command to run. options are: dobulk, createproject, listprojects, import, listdocs, watch, read, updatedoc")
	projectApiKey := flag.String("projectapi", "", "project apikey")
	keyName := flag.String("key", "", "key")
	value := flag.String("value", "", "value")
	docName := flag.String("doc", "", "doc key")
	importFile := flag.String("import", "", "file to import document from")
	certFile := flag.String("cert", "", "file path to cert")
	noDocs := flag.Int("nodocs", 20, "number of docs to generate when doing bulk updates.")
	noUpdates := flag.Int("noupdates", 1000, "number of updates per document")
	concurrent := flag.Int("concurrent", 1, "number of concurrent clients to use per document")
	createProjectPerDoc := flag.Bool("projectperdoc", false, "create project per doc for bulk test")
	watchIdleDocs := flag.Bool("watchidle", false, "watch for idle (non updated) docs. This means connection/attach is done but no updates are expected")
	syncDocs := flag.Bool("syncdocs", false, "when watching docs, sync doc and not just be notified of changes")
	ip := flag.String("ip", "10.0.0.39", "yorkie ip")
	clientPort := flag.String("port", "8080", "client connection port")

	flag.Parse()

	start := time.Now()
	defer func() {
		fmt.Printf("\n\ntotal time %d seconds\n", int(time.Since(start).Seconds()))
	}()

	yt := NewYorkieTest(*ip, *clientPort, *projectApiKey, *noDocs, *certFile)

	yt.adminLogin()

	// if we have a project, then connect
	if *projectApiKey != "" {
		yt.connectClient(*certFile)
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

	// need to add parameter validation. TODO(kpfaulkner)
	switch *commandToRun {
	case "dobulk":
		yt.doBulk(*msSleep, *concurrent, *noDocs, *noUpdates, *createProjectPerDoc)
		return
	case "createproject":
		err := yt.createProject(*projectName)
		if err != nil {
			fmt.Printf("create project error %s\n", err.Error())
		}
		return
	case "listprojects":
		err := yt.listProjects()
		if err != nil {
			fmt.Printf("list project error %s\n", err.Error())
		}
		return
	case "import":
		err := yt.importFile(*importFile, doc)
		if err != nil {
			log.Printf("importFile err %s", err.Error())
		}
		return
	case "listdocs":
		err := yt.listDocumentsForProject(*projectName)
		if err != nil {
			fmt.Printf("admin err %s\n", err.Error())
		}
		return
	case "watch":
		yt.watchDocs(*noDocs, *projectApiKey, *certFile, *createProjectPerDoc, *syncDocs, *watchIdleDocs)
		return
	case "read":
		err := yt.readDoc(doc)
		if err != nil {
			log.Printf("read doc error %s", err.Error())
		}
		return
	case "updatedoc":
		if *keyName != "" && *value != "" {
			yt.updateDoc(doc, *keyName, *value, 1)
			err = yt.cli.Detach(yt.ctx, doc)
			if err != nil {
				fmt.Printf("unable to detach : %s\n", err.Error())
			}
			return
		} else {
			fmt.Printf("unable to update. Either key or value are blank\n")
			return
		}
	default:
		fmt.Printf("unknown command %s\n", *commandToRun)
	}
}
