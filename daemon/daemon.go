package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/brett19/eventing-router/daemon/cepnet"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

const (
	DefaultRetainPeriod = 5 * time.Minute
)

var InstanceID string
var Events *EventQueue

type ErrorJSON struct {
	Error struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func jsonifyError(err error) ErrorJSON {
	jsonErr := ErrorJSON{}
	jsonErr.Error.Message = err.Error()
	return jsonErr
}

func writeJSONError(w http.ResponseWriter, err error) {
	jsonErr := jsonifyError(err)

	jsonBytes, err := json.Marshal(jsonErr)
	if err != nil {
		log.Printf("Failed to marshal error JSON: %s", err)
		w.WriteHeader(500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(400)
	w.Write(jsonBytes)
}

func HttpEventPublish(w http.ResponseWriter, req *http.Request) {
	bytes, err := ioutil.ReadAll(req.Body)
	if err != nil {
		writeJSONError(w, err)
		return
	}

	Events.Publish(bytes)
	w.WriteHeader(200)
}

func HandleEventWatch(w http.ResponseWriter, req *http.Request) {
	// Read the query parameters
	q := req.URL.Query()
	fromInstance := q.Get("fromInstance")
	fromIndexStr := q.Get("fromIndex")

	var fromIndex uint64
	if fromIndexStr != "" {
		fromIndexParsed, err := strconv.ParseUint(fromIndexStr, 10, 64)
		if err != nil {
			writeJSONError(w, err)
			return
		}

		fromIndex = fromIndexParsed
	}

	// Validate that if a starting index is specified, that they also specified
	// the specific instance the index came from (since it has to match)
	if fromIndex > 0 {
		if fromInstance == "" {
			err := errors.New("you must specify the instance with an index")
			writeJSONError(w, err)
			return
		}
	}

	// Validate that the clients instance matches ours if they specify it
	if fromInstance != "" && fromInstance != InstanceID {
		err := errors.New("the server instance has changed")
		writeJSONError(w, err)
		return
	}

	watcher, err := Events.NewWatcher(fromIndex)
	if err != nil {
		writeJSONError(w, err)
		return
	}

	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(200)

	closeNotifier := w.(http.CloseNotifier)
EventLoop:
	for {
		var poppedEvent *EventItem
		if closeNotifier != nil {
			select {
			case <-closeNotifier.CloseNotify():
				break EventLoop
			case event := <-watcher.Events:
				poppedEvent = &event
			}
		} else {
			event := <-watcher.Events
			poppedEvent = &event
		}

		if poppedEvent != nil {
			var dataToWrite []byte
			dataToWrite = append(dataToWrite, poppedEvent.Data...)
			dataToWrite = append(dataToWrite, []byte("\n")...)

			_, err := w.Write(dataToWrite)
			if err != nil {
				break EventLoop
			}

			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
	}

	watcher.Close()
}

type FilterDef []byte

type ControlMessage interface{}

type StateControlMessage struct {
	StartStop bool
}

type FilterControlMessage struct {
	FilterID uint16
	Filter   FilterDef
}

type CepWatchManager struct {
	state        uint32
	channel      *cepnet.ConnChannel
	controlCh    chan ControlMessage
	filters      map[uint16]FilterDef
	watcher      *EventWatcher
	shutdownSig  chan struct{}
	watchDoneSig chan struct{}
}

func (manager *CepWatchManager) watchControl(msg ControlMessage) {
	switch msg := msg.(type) {
	case FilterControlMessage:
		if msg.Filter == nil {
			delete(manager.filters, msg.FilterID)
			manager.channel.WriteMessage(&cepnet.PushStreamRemoveFilterMessage{
				FilterID: msg.FilterID,
			})
		} else {
			manager.filters[msg.FilterID] = msg.Filter
			manager.channel.WriteMessage(&cepnet.PushStreamRemoveFilterMessage{
				FilterID: msg.FilterID,
			})
		}
	}
}

func (manager *CepWatchManager) watchEvent(event *EventItem) {
	manager.channel.WriteMessage(&cepnet.PushStreamItemMessage{
		EventIndex: event.Id,
		Data:       event.Data,
	})
}

func (manager *CepWatchManager) watchRoutine(fromIndex uint64) {
	watcher, err := Events.NewWatcher(fromIndex)
	if err != nil {
		log.Printf("events watcher failed to start: %s", err)
	}

	for {
		select {
		case <-manager.shutdownSig:
			// Stop receiving new messages as a watcher
			watcher.Close()

			// We have to clean up all our control messages before we can shut down.
		CleanupLoop:
			for {
				select {
				case message := <-manager.controlCh:
					manager.watchControl(message)
				default:
					break CleanupLoop
				}
			}

			manager.channel.WriteMessage(&cepnet.PushStreamEndMessage{})

			close(manager.watchDoneSig)

		case message := <-manager.controlCh:
			manager.watchControl(message)
		case event := <-watcher.Events:
			manager.watchEvent(&event)
		}

	}
}

func (manager *CepWatchManager) Start(fromIndex uint64) error {
	shouldStart := atomic.CompareAndSwapUint32(&manager.state, 0, 1)
	if !shouldStart {
		return errors.New("cannot start a started stream")
	}

	if shouldStart {
		go manager.watchRoutine(fromIndex)
	}

	return nil
}

func (manager *CepWatchManager) Stop() {
	manager.shutdownSig <- struct{}{}
}

func (manager *CepWatchManager) SetFilter(filterID uint16, filter []byte) error {
	manager.controlCh <- FilterControlMessage{
		FilterID: filterID,
		Filter:   filter,
	}
	return nil
}

func writeCepError(channel *cepnet.ConnChannel, err error) {
	msg := &cepnet.ErrorMessage{
		Code:    0,
		Message: err.Error(),
	}

	writeErr := channel.WriteMessage(msg)
	if writeErr != nil {
		log.Printf("Failed to write error message to channel: %s", writeErr)
	}
}

func writeCepSuccess(channel *cepnet.ConnChannel) {
	msg := &cepnet.SuccessMessage{}

	writeErr := channel.WriteMessage(msg)
	if writeErr != nil {
		log.Printf("Failed to write success message to channel: %s", writeErr)
	}
}

func handleCepChannel(channel *cepnet.ConnChannel) {
	watchManager := &CepWatchManager{
		channel: channel,
	}

	for {
		msg, err := channel.ReadMessage()
		if err != nil {
			log.Printf("CEP Channel %p failed to read message: %s", channel, err)
			break
		}

		log.Printf("CEP Channel %p received message %T %+v", channel, msg, msg)

		switch msg := msg.(type) {
		case *cepnet.StreamAddFilterMessage:
			watchManager.SetFilter(msg.FilterID, msg.Filter)
			writeCepSuccess(channel)
		case *cepnet.StreamRemoveFilterMessage:
			watchManager.SetFilter(msg.FilterID, nil)
			writeCepSuccess(channel)
		case *cepnet.StreamStartMessage:
			err := watchManager.Start(0)
			if err != nil {
				writeCepError(channel, err)
				break
			}
			writeCepSuccess(channel)
		case *cepnet.StreamRecoverMessage:
			if msg.InstanceID != InstanceID {
				writeCepError(channel, errors.New("Invalid instance ID"))
				break
			}

			err := watchManager.Start(msg.FromIndex)
			if err != nil {
				writeCepError(channel, err)
				break
			}
			writeCepSuccess(channel)
		case *cepnet.StreamStopMessage:
			watchManager.Stop()
			writeCepSuccess(channel)
		}
	}

	log.Printf("CEP channel %p shut down", channel)
}

func handleCepClient(client *cepnet.Conn) {
	log.Printf("CEP client %p starting", client)

	for {
		channel, err := client.AcceptChannel()
		if err != nil {
			log.Printf("CEP client %p failed to accept channel: %s", client, err)
			break
		}

		log.Printf("CEP client %p opened channel %p", client, channel)
		go handleCepChannel(channel)
	}

	log.Printf("CEP client %p shut down", client)
}

func Start() {
	// Set up a unique instance-id to correlate the logId's from this instance
	instanceID, err := uuid.NewRandom()
	if err != nil {
		log.Fatalf("Failed to generate a random UUID for this instance")
		return
	}
	InstanceID = instanceID.String()

	// Set up the queue itself
	Events = NewEventQueue(DefaultRetainPeriod)

	r := mux.NewRouter()
	r.HandleFunc("/events/publish", HttpEventPublish)
	r.HandleFunc("/events/watch", HandleEventWatch)

	httpServer := &http.Server{
		Addr:           ":8098",
		Handler:        r,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	cepServer, err := cepnet.Listen(":8099")
	if err != nil {
		log.Printf("Failed to start CEP service: %s", err)
		os.Exit(1)
	}

	// Some signals to keep track of shutdown
	httpClosedSig := make(chan struct{})
	cepClosedSig := make(chan struct{})

	// Start up our HTTP service
	go func() {
		log.Printf("Starting http listener on %s", httpServer.Addr)
		httpServer.ListenAndServe()

		log.Printf("Http service has stopped.")
		httpClosedSig <- struct{}{}
	}()

	// Start up our CEP service
	go func() {
		log.Printf("Starting CEP listener on %s", cepServer.Addr())

		for {
			client, err := cepServer.Accept()
			if err != nil {
				log.Printf("Failed to accept CEP client: %s", err)
				break
			}

			go handleCepClient(client)
		}

		log.Printf("CEP service has stopped.")
		cepClosedSig <- struct{}{}
	}()

	// Start up a goroutine to publish messages for debugging...
	go func() {
		for i := 1; ; i++ {
			time.Sleep(1 * time.Second)

			jsonMsg := fmt.Sprintf(`{"hello": "world", "x": %d}`, i)
			Events.Publish([]byte(jsonMsg))
		}
	}()

	// Wait for a close signal and shut down
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	go func() {
		<-c
		os.Exit(-1)
	}()

	// Close all our services
	log.Printf("Server shutting down gracefully")
	httpServer.Close()
	cepServer.Close()

	// Wait for all services to shut down
	<-httpClosedSig
	<-cepClosedSig

	// Indicate to the user that everything went well
	log.Printf("Graceful shutdown completed")
}
