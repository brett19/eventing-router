package daemon

import (
	"container/list"
	"errors"
	"log"
	"sync"
	"time"
)

const (
	QueueCleanupPeriod = 5 * time.Second
)

type EventItem struct {
	Id        uint64
	CreatedAt time.Time
	Data      []byte
}

type EventQueue struct {
	retainPeriod   time.Duration
	logLock        sync.Mutex
	logCounter     uint64
	logEvents      *list.List
	cleanupIndex   uint64
	shutdownSig    chan struct{}
	cleanupDoneSig chan struct{}
	watcherWake    *sync.Cond
}

func NewEventQueue(retainPeriod time.Duration) *EventQueue {
	queue := &EventQueue{
		retainPeriod:   retainPeriod,
		logCounter:     1,
		logEvents:      list.New(),
		shutdownSig:    make(chan struct{}),
		cleanupDoneSig: make(chan struct{}),
	}
	queue.watcherWake = sync.NewCond(&queue.logLock)

	go queue.cleanupRoutine()

	return queue
}

func (queue *EventQueue) Close() error {
	close(queue.shutdownSig)
	<-queue.cleanupDoneSig
	return nil
}

func (queue *EventQueue) cleanupRoutine() {
	for {
		select {
		case <-queue.shutdownSig:
			return
		case <-time.After(QueueCleanupPeriod):
		}

		cleanupTime := time.Now().Add(-queue.retainPeriod)

		queue.logLock.Lock()
		qitem := queue.logEvents.Front()
		for qitem != nil {
			nitem := qitem.Next()

			item := qitem.Value.(*EventItem)
			if item.CreatedAt.Before(cleanupTime) {
				queue.logEvents.Remove(qitem)
				queue.cleanupIndex = item.Id
			}

			qitem = nitem
		}
		queue.logLock.Unlock()
	}
}

// EventsSince will return up to 10 items that have occured since the marked index
func (queue *EventQueue) eventsSinceRLocked(index uint64) ([]EventItem, error) {
	//log.Printf("Fetching events since %d", index)

	// If we've already cleaned up past the beginning section, lets just
	// give up early since our search for the start point is guarenteed
	// to fail.
	if index <= queue.cleanupIndex {
		return nil, errors.New("reader is too far behind")
	}

	// Lets find the appropriate start point based on the index
	//log.Printf("-- Locating start-point")
	var sitem *list.Element
	qitem := queue.logEvents.Back()
	for qitem != nil {
		item := qitem.Value.(*EventItem)
		if item.Id <= index {
			break
		}

		sitem = qitem

		qitem = qitem.Prev()
	}

	// Loop from the startpoint we found forward to insert
	// items into our events list.  We do this separately to
	// avoid having the array backwards.
	//log.Printf("-- Building events list")
	var events []EventItem
	qitem = sitem
	for qitem != nil {
		item := qitem.Value.(*EventItem)
		events = append(events, *item)
		qitem = qitem.Next()
	}

	//log.Printf("-- Done")
	return events, nil
}

func (queue *EventQueue) Publish(jsonBytes []byte) error {
	//log.Printf("Publishing an event (len:%d)", len(jsonBytes))

	queue.logLock.Lock()

	queue.logCounter++
	thisEventId := queue.logCounter

	//log.Printf("-- Selected index %d", thisEventId)

	item := &EventItem{
		Id:        thisEventId,
		CreatedAt: time.Now(),
		Data:      jsonBytes,
	}
	queue.logEvents.PushBack(item)
	//log.Printf("-- Pushed to list")

	queue.logLock.Unlock()

	queue.watcherWake.Broadcast()

	//log.Printf("-- Done")
	return nil
}

type EventWatcher struct {
	parent        *EventQueue
	currentIndex  uint64
	Events        chan EventItem
	shutdownSig   chan struct{}
	listenDoneSig chan struct{}

	// Holds any errors that occured while listening, it is not
	// safe to read this value unless the events channel is closed.
	listenError error
}

func (watcher *EventWatcher) listenRoutine() {
	queue := watcher.parent

	for {
		select {
		case <-watcher.shutdownSig:
			break
		default:
		}

		//log.Printf("Event watcher %p scanning", watcher)

		queue.logLock.Lock()
		events, err := watcher.parent.eventsSinceRLocked(watcher.currentIndex)
		if err != nil {
			queue.logLock.Unlock()
			watcher.listenError = err
			close(watcher.Events)
			break
		}

		if len(events) == 0 {
			// Wait for more events, this wait will implicitly unlock the
			// queues lock while it runs
			//log.Printf("Event watcher %p sleeping", watcher)
			watcher.parent.watcherWake.Wait()
			//log.Printf("Event watcher %p woken", watcher)

			queue.logLock.Unlock()
			continue
		}

		queue.logLock.Unlock()

		log.Printf("Event watcher %p found %d events", watcher, len(events))

		for _, event := range events {
			//log.Printf("Event watcher %p sending event %d", event.Id)

			select {
			case <-watcher.shutdownSig:
				// If the shutdown signal is tripped (close), we will break out of here,
				// and subsequently break out of the top loop from the check at the top
				break
			case watcher.Events <- event:
				//log.Printf("Event watcher %p sent event", watcher)
				watcher.currentIndex = event.Id
			}
		}
	}

	watcher.listenDoneSig <- struct{}{}
}

func (watcher *EventWatcher) Close() error {
	log.Printf("Shutting down event watcher %p", watcher)

	// Signal this watcher to shut down, we have to use a bit for
	close(watcher.shutdownSig)

	// Signal the parents watcherWake to wake everyone up so that this
	// watcher will get unblocked for shutdown detection
	watcher.parent.watcherWake.Broadcast()

	// Wait until the watcher has actually shut down its listener
	<-watcher.listenDoneSig

	return nil
}

func (queue *EventQueue) NewWatcher(fromIndex uint64) (*EventWatcher, error) {
	if fromIndex == 0 {
		queue.logLock.Lock()
		fromIndex = queue.logCounter
		queue.logLock.Unlock()
	}

	watcher := &EventWatcher{
		parent:        queue,
		currentIndex:  fromIndex,
		Events:        make(chan EventItem),
		shutdownSig:   make(chan struct{}),
		listenDoneSig: make(chan struct{}),
	}

	go watcher.listenRoutine()

	log.Printf("Created new event watcher %v", watcher)
	return watcher, nil
}
