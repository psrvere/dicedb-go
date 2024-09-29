# go-dice

`go-dice` is a high-performance, fully-featured Go client designed specifically for interacting with DiceDB, a powerful, distributed in-memory database.
This library allows Go developers to easily connect to DiceDB, execute commands, and manage their data efficiently.

As a fork of [go-redis](https://github.com/redis/go-redis) client, go-dice maintains compatibility with DiceDB's extended features, including native support for DiceDB commands and operations.

## Features

- Seamless integration with DiceDB’s extended command set.
- Fully extensible to support DiceDB’s unique data structures.
- Optimized for high-performance and large-scale applications.
- Easy-to-use APIs for common operations like string manipulation, set operations, and more.


## Get started

### Installation

To install `go-dice` in your Go project, you can use go get to fetch the library:
```shell
go get github.com/DiceDB/go-dice
```

After installing, you can import the library into your project:
```go
import (
	"github.com/dicedb/go-dice"
)
```

### Quickstart

```go
package main

import (
   "context"
   "fmt"
   "log"

   dice "github.com/dicedb/go-dice"
)

func main() {
   // Create a new DiceDB client
   client := dice.NewClient(&dice.Options{
      Addr:     "localhost:7379", // Replace with your DiceDB server address
      Password: "",               // No password set
      DB:       0,                // Use default DB
   })

   // Use context for operations
   ctx := context.Background()

   // Set a key-value pair
   err := client.Set(ctx, "key", "value", 0).Err()
   if err != nil {
      log.Fatalf("Failed to set key: %v", err)
   }

   // Retrieve the value of the key
   val, err := client.Get(ctx, "key").Result()
   if err != nil {
      log.Fatalf("Failed to get key: %v", err)
   }

   fmt.Printf("key: %s\n", val)

   // Close the client connection when done
   err = client.Close()
   if err != nil {
      log.Fatalf("Error closing the client: %v", err)
   }
}
```
This basic example demonstrates how you can interact with DiceDB using `go-dice`. You can explore more advanced usage by referring to the DiceDB [Documentation](https://dicedb.io/get-started/installation/).

You can also refer examples [here]((https://github.com/DiceDB/dice/tree/master/examples/leaderboard-go)) implement truly real-time applications like Leaderboard with simple SQL query.

### Setting up repository from source for development and contributions

To run `go-dice` for local development or running from source, you will need:
1. [Golang](https://go.dev/)
2. Any of the below supported platform environment:
    1. [Linux based environment](https://en.wikipedia.org/wiki/Comparison_of_Linux_distributions)
    2. [OSX (Darwin) based environment](https://en.wikipedia.org/wiki/MacOS)
    3. WSL under Windows

```
$ git clone https://github.com/DiceDB/go-dice.git
$ cd go-dice
```

## How to contribute

The Code Contribution Guidelines are published at [CONTRIBUTING.md](CONTRIBUTING.md); please read them before you start making any changes. This would allow us to have a consistent standard of coding practices and developer experience.

Contributors can join the [Discord Server](https://discord.gg/6r8uXWtXh7) for quick collaboration.

## License
`go-dice` is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
