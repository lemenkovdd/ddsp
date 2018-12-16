package frontend

import (
	"sync"
	"time"

	rclient "router/client"
	"router/router"
	"storage"
)

// InitTimeout is a timeout to wait after unsuccessful List() request to Router.
//
// InitTimeout -- количество времени, которое нужно подождать до следующей попытки
// отправки запроса List() в Router.
const InitTimeout = 100 * time.Millisecond

// Config stores configuration for a Frontend service.
//
// Config -- содержит конфигурацию Frontend.
type Config struct {
	// Addr is an address to listen at.
	// Addr -- слушающий адрес Frontend.
	Addr storage.ServiceAddr
	// Router is an address of Router service.
	// Router -- адрес Router service.
	Router storage.ServiceAddr

	// NC specifies client for Node.
	// NC -- клиент для node.
	NC storage.Client `yaml:"-"`
	// RC specifies client for Router.
	// RC -- клиент для router.
	RC rclient.Client `yaml:"-"`
	// NodesFinder specifies a NodeFinder to use.
	// NodesFinder -- NodesFinder, который нужно использовать в Frontend.
	NF router.NodesFinder `yaml:"-"`
}

// Frontend is a frontend service.
type Frontend struct {
	conf        Config
	initOnce    sync.Once
	routerNodes []storage.ServiceAddr
}

// New creates a new Frontend with a given cfg.
//
// New создает новый Frontend с данным cfg.
func New(cfg Config) *Frontend {
	return &Frontend{
		conf: cfg,
	}
}

func (fe *Frontend) applyPutDel(k storage.RecordID, method func(node storage.ServiceAddr) error) error {
	nodes, err := fe.conf.RC.NodesFind(fe.conf.Router, k)
	if err != nil {
		return err
	}

	if len(nodes) < storage.MinRedundancy {
		return storage.ErrNotEnoughDaemons
	}

	results := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node storage.ServiceAddr) {
			results <- method(node)
		}(node)
	}

	okCount := 0
	errCounts := make(map[error]int)

	for range nodes {
		err := <-results
		if err == nil {
			okCount++
		} else {
			errCounts[err]++
		}
	}

	if okCount >= storage.MinRedundancy {
		return nil
	}

	for err, count := range errCounts {
		if count >= storage.MinRedundancy {
			return err
		}
	}

	return storage.ErrQuorumNotReached
}

// Put an item to the storage if an item for the given key doesn't exist.
// Returns error otherwise.
//
// Put -- добавить запись в хранилище, если запись для данного ключа
// не существует. Иначе вернуть ошибку.
func (fe *Frontend) Put(k storage.RecordID, d []byte) error {
	return fe.applyPutDel(k, func(node storage.ServiceAddr) error {
		return fe.conf.NC.Put(node, k, d)
	})
}

// Del an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Del -- удалить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Del(k storage.RecordID) error {
	return fe.applyPutDel(k, func(node storage.ServiceAddr) error {
		return fe.conf.NC.Del(node, k)
	})
}

// Get an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Get -- получить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Get(k storage.RecordID) ([]byte, error) {
	fe.initOnce.Do(func() {
		for {
			nodes, err := fe.conf.RC.List(fe.conf.Router)
			if err == nil {
				fe.routerNodes = nodes
				break
			}
			time.Sleep(InitTimeout)
		}
	})

	nodes := fe.conf.NF.NodesFind(k, fe.routerNodes)

	type result struct {
		data []byte
		err  error
	}
	results := make(chan result, len(nodes))

	// Make method calls asynchronously
	for _, node := range nodes {
		go func(node storage.ServiceAddr) {
			data, err := fe.conf.NC.Get(node, k)
			results <- result{data: data, err: err}
		}(node)
	}

	// Collect and process results of requests
	dataCounts := make(map[string]int)
	errCounts := make(map[error]int)

	for range nodes {
		result := <-results

		if result.err != nil {
			errCounts[result.err]++
			if errCounts[result.err] >= storage.MinRedundancy {
				return nil, result.err
			}
			continue
		}

		dataKey := string(result.data)
		dataCounts[dataKey]++
		if dataCounts[dataKey] >= storage.MinRedundancy {
			return result.data, nil
		}
	}

	return nil, storage.ErrQuorumNotReached
}
