package internal

type RWMutex struct {
	write   chan struct{}
	readers chan int
}

func NewRWMutex() *RWMutex {
	return &RWMutex{
		// This is used as a normal Mutex.
		write: make(chan struct{}, 1),
		// This is used to protect the readers count.
		// By receiving the value it is guaranteed that no
		// other goroutine is changing it at the same time.
		readers: make(chan int, 1),
	}
}

func (l RWMutex) Lock() {
	l.write <- struct{}{}
}
func (l RWMutex) Unlock() {
	<-l.write
}

func (l RWMutex) RLock() {
	// Count current readers. Default to 0.
	var rs int
	// Select on the channels without default.
	// One and only one case will be selected and this
	// will block until one case becomes available.
	select {
	case l.write <- struct{}{}: // One sending case for write.
		// If the write lock is available we have no readers.
		// We grab the write lock to prevent concurrent
		// read-writes.
	case rs = <-l.readers: // One receiving case for read.
		// There already are readers, let's grab and update the
		// readers count.
	}
	// If we grabbed a write lock this is 0.
	rs++
	// Updated the readers count. If there are none this
	// just adds an item to the empty readers channel.
	l.readers <- rs
}

func (l RWMutex) RUnlock() {
	// Take the value of readers and decrement it.
	rs := <-l.readers
	rs--
	// If zero, make the write lock available again and return.
	if rs == 0 {
		<-l.write
		return
	}
	// If not zero just update the readers count.
	// 0 will never be written to the readers channel,
	// at most one of the two channels will have a value
	// at any given time.
	l.readers <- rs
}

func (l RWMutex) TryLock() bool {
	select {
	case l.write <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l RWMutex) TryRLock() bool {
	var rs int
	select {
	case l.write <- struct{}{}:
	case rs = <-l.readers:
	default:
		// Failed to lock, do not update anything.
		return false
	}
	rs++
	l.readers <- rs
	return true
}
