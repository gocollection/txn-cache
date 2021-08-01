package lock

type Mutex struct {
	in chan struct{}
}

func NewMutex() *Mutex {
	return &Mutex{make(chan struct{}, 1)}
}

// Lock is blocking effort to acquire the lock
// if already taken, it would wait for it to be released
func (m *Mutex) Lock() {
	m.in <- struct{}{}
}

// Unlock releases the lock if already taken
// blocking if lock is in unlocked state
func (m *Mutex) Unlock() {
	<-m.in
}

// TryLock tries to acquire the lock in non blocking fashion
// return instantly with success or failure
func (m *Mutex) TryLock() bool {
	select {
	case m.in <- struct{}{}:
		return true
	default:
		return false
	}
}
