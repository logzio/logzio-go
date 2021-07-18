package inMemoryQueue

import (
	"errors"
)

var (
	// ErrEmpty is returned when the stack or queue is empty.
	ErrEmpty = errors.New("inMemoryQueue.go: queue is empty")
)
