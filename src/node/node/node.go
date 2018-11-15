package node

import (
	"sync"
	"time"

	router "router/client"
	"storage"
)

// Config stores configuration for a Node service.
//
// Config -- содержит конфигурацию Node.
type Config struct {
	// Addr is an address to listen at.
	// Addr -- слушающий адрес Node.
	Addr storage.ServiceAddr
	// Router is an address of Router service.
	// Router -- адрес Router service.
	Router storage.ServiceAddr
	// Heartbeat is a time interval between heartbeats.
	// Heartbeat -- интервал между двумя heartbeats.
	Heartbeat time.Duration

	// Client specifies client for Router.
	// Client -- клиент для Router.
	Client router.Client `yaml:"-"`
}

// Node is a Node service.
type Node struct {
	conf      Config
	heartbeat chan struct{}
	storage   map[storage.RecordID][]byte
	lock      sync.RWMutex
}

// New creates a new Node with a given cfg.
//
// New создает новый Node с данным cfg.
func New(cfg Config) *Node {
	return &Node{
		conf:      cfg,
		heartbeat: make(chan struct{}),
		storage:   make(map[storage.RecordID][]byte),
	}
}

// Heartbeats runs heartbeats from node to a router
// each time interval set by cfg.Heartbeat.
//
// Heartbeats запускает отправку heartbeats от node к router
// через каждый интервал времени, заданный в cfg.Heartbeat.
func (node *Node) Heartbeats() {
	go func() {
		for {
			select {
			case <-node.heartbeat:
				return
			default:
				node.conf.Client.Heartbeat(node.conf.Router, node.conf.Addr)
				time.Sleep(node.conf.Heartbeat)
			}
		}
	}()
}

// Stop stops heartbeats
//
// Stop останавливает отправку heartbeats.
func (node *Node) Stop() {
	node.heartbeat <- struct{}{}
}

// Put an item to the node if an item for the given key doesn't exist.
// Returns the storage.ErrRecordExists error otherwise.
//
// Put -- добавить запись в node, если запись для данного ключа
// не существует. Иначе вернуть ошибку storage.ErrRecordExists.
func (node *Node) Put(k storage.RecordID, d []byte) error {
	node.lock.Lock()
	defer node.lock.Unlock()

	if _, ok := node.storage[k]; ok {
		return storage.ErrRecordExists
	}
	node.storage[k] = d

	return nil
}

// Del an item from the node if an item exists for the given key.
// Returns the storage.ErrRecordNotFound error otherwise.
//
// Del -- удалить запись из node, если запись для данного ключа
// существует. Иначе вернуть ошибку storage.ErrRecordNotFound.
func (node *Node) Del(k storage.RecordID) error {
	node.lock.Lock()
	defer node.lock.Unlock()

	if _, ok := node.storage[k]; !ok {
		return storage.ErrRecordNotFound
	}
	delete(node.storage, k)

	return nil
}

// Get an item from the node if an item exists for the given key.
// Returns the storage.ErrRecordNotFound error otherwise.
//
// Get -- получить запись из node, если запись для данного ключа
// существует. Иначе вернуть ошибку storage.ErrRecordNotFound.
func (node *Node) Get(k storage.RecordID) ([]byte, error) {
	node.lock.RLock()
	defer node.lock.RUnlock()

	if item, ok := node.storage[k]; ok {
		return item, nil
	}

	return nil, storage.ErrRecordNotFound
}
