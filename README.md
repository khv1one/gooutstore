# gooutstore [![GoDoc](https://godoc.org/github.com/khv1one/gooutstore?status.png)](https://pkg.go.dev/github.com/khv1one/gooutstore)

gooutstore is a library for managing outbox messages in a distributed system. It provides interfaces and implementations for dispatching and generating outbox messages using various database clients.

## Installation

To install gooutstore, use `go get`:

```sh
go get github.com/khv1one/gooutstore
```

## Usage

### Dispatcher

The `Dispatcher` is responsible for dispatching outbox messages. It reads messages from the outbox, processes them, and updates their status.

### Generator

The `Generator` is responsible for generating outbox messages and storing them in the database.

### Examples

#### Example dispatcher

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/khv1one/gooutstore"
    "github.com/khv1one/gooutstore/client/pgpq"
)

func main() {
    db, err := sql.Open("postgres", "your-database-connection-string")
    if err != nil {
        log.Fatal(err)
    }

    dispatcher := gooutstore.NewDispatcherWithClient(pgpq.NewClient(db), processMessage)

    ctx := context.Background()
    if err := dispatcher.Start(ctx); err != nil {
        log.Fatal(err)
    }
}

func processMessage(ctx context.Context, msg gooutstore.IOutboxMessage) error {
    // Process the message
    return nil
}
```

#### Example generator

```go
package main

import (
    "context"
    "log"

    "github.com/khv1one/gooutstore"
    "github.com/khv1one/gooutstore/client/pgpq"
)

func main() {
    db, err := sql.Open("postgres", "your-database-connection-string")
    if err != nil {
        log.Fatal(err)
    }

    generator := gooutstore.NewGeneratorWithClient(pgpq.NewClient(db))

    ctx := context.Background()
    messages := []gooutstore.IOutboxMessage{
        ExampleMessage{ID: 1},
        ExampleMessage{ID: 2},
    }

    if err := generator.Send(ctx, messages...); err != nil {
        log.Fatal(err)
    }
}

type ExampleMessage struct {
    ID int
}

func (m *ExampleMessage) Key() string {
    return "key"
}

func (m *ExampleMessage) Type() string {
    return "type"
}
```

## Interfaces
WIP

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.