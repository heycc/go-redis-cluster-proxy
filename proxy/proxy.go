package proxy

type Error string

func (err Error) Error() string { return string(err) }

type Conn interface {
	//Close() error

	Do(cmd string, args ...interface{}) (reply interface{}, err error)

	//Send(commandName string, args ...interface{}) error

	// Flush flushes the output buffer to the Redis server.
	//Flush() error

	// Receive receives a single reply from the Redis server
	//Receive() (reply interface{}, err error)
}
