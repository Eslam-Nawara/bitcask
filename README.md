# Bitcask
Bitcask is an append-only key/value data storage engine. The origin of Bitcask is tied to the Riak distributed database system.

**NOTE:** All project specifications and usage are mentioned in the [Official Bitcask Design Paper](https://riak.com/assets/bitcask-intro.pdf)

## Bitcask package

- ### Get the package:
```sh
go get github.com/IslamWalid/bitcask
```
- ### Package:
| Config Option                                                 | Description                                            |
|---------------------------------------------------------------|--------------------------------------------------------|
| `ReadWrite` | Gives a read and write permissions on the specified datastore. |
| `ReadOnly` | Gives a read only permission on the specified datastore. |
| `SyncOnPut` | Forces the data to be written directly to the datastore data files on every write operation, it is prefered to use this option only in cases of very sensitive data since all the data is flushed to the disk and won't be lost on catastrophic damages to the system. |
| `SyncOnDemand` | Gives the user the control when to flush the data to the disk by using ```Sync```, data is flushed automatically when ```Close``` is called or whenever the process terminates or fails, it is generally good option since it makes write and read operations much more faster. |

| Functions and Methods                                                     | Description                                |
|---------------------------------------------------------------|--------------------------------------------------------|
| `func Open(dirPath string, opts ...ConfigOpt) (*Bitcask, error)` | Open a new or an existing bitcask datastore. |
| `func (bitcask *Bitcask) Put(key string, value string) error` | Stores a key and a value in the bitcask datastore. |
| `func (bitcask *Bitcask) Get(key string) (string, error)` | Reads a value by key from a datastore. |
| `func (bitcask *Bitcask) Delete(key string) error` | Removes a key from the datastore. |
| `func (bitcask *Bitcask) Close()` | Close a bitcask data store and flushes all pending writes to disk. |
| `func (bitcask *Bitcask) ListKeys() []string` | Returns list of all keys. |
| `func (bitcask *Bitcask) Sync() error` | Force any writes to sync to disk. |
| `func (bitcask *Bitcask) Merge() error` | Reduces the disk usage by removing old and deleted values from the datafiles. Also, produce hintfiles for faster startup. |
| `func (bitcask *Bitcask) Fold(fun func(string, string, any) any, acc any) any` | Fold over all K/V pairs in a Bitcask datastore.→ Acc Fun is expected to be of the form: F(K,V,Acc0) → Acc. |

- ### Usage Example:
```go
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/IslamWalid/bitcask"
)

func main() {
	b, err := bitcask.Open("datastore", bitcask.ReadWrite)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	go func() {
		for i := 0; i < 10000; i++ {
			key := fmt.Sprintf("key%d", i+1)
			value := fmt.Sprintf("value%d", i+1)
			err := b.Put(key, value)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
	}()

	// Sleep time simulate another work to be done by the program
	time.Sleep(time.Second)

	// Perform merge (if needed) at the end of the program
	err = b.Merge()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```
- **Important Notes:**
    - `Put`, `Get`, `Delete` and `Sync` are blocking calls as they deals with I/O, so - whenever possible - it is a good idea to make a goroutine handles these calls and continue on the rest of the program.
    - `Merge` is also a blocking call like the mentioned above, but more slower since it works on all the data to reduce its size, so it prefered to use it when all writing operations is done. If there's another work to be done by the process, using a goroutine to handle the call will be a good idea as well.

## Resp Server Package
The main idea is to implement a resp server to enable communicating with any remote bitcask datastore instanse using a client supports [resp protocol](https://redis.io/docs/reference/protocol-spec/), eg: `redis-cli`.
- ### Get the package:
```sh
go get github.com/IslamWalid/bitcask/pkg/respserver
```
- ### Package:
| Functions and Methods                                                 | Description                                            |
|---------------------------------------------------------------|--------------------------------------------------------|
| `func New(dataStoreDir, port string) (*RespServer, error)`| New creates new resp server object listening in the given port and using a datastore in the given directory path. |
| `func (r *RespServer) ListenAndServe() error`| ListenAndServe registers the needed handlers then starts the server. |
| `func (r *RespServer) Close()`| Close closes the used bitcask datastore. |

- ### Usage Example:
```go
package main

import (
	"fmt"
	"os"

	resp "github.com/IslamWalid/bitcask/pkg/respserver"
)

func main() {
	s, err := resp.New("./datastore", ":12345")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	err = s.ListenAndServe()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

## Bitcask Server
A program that uses [resp server package](#resp-server-package) to start a bitcask server.
- ### Installation:
```sh
go install github.com/IslamWalid/bitcask/cmd/bitserver@latest
```

- ### Usage:
    - Run the server:
    ```sh
    bitserver -d <datastore_path> -p <port>
    ```
    - Connect to it using redis-cli
    ```sh
    redis-cli -p <port>
    ```
    **note:** both `bitserver` and `redis-cli` use `6379` as the default port in case `-p` is not specified.
