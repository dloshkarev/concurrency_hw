package primitive

type Semaphore struct {
	tickets chan struct{}
}

func NewSemaphore(ticketsCount int) *Semaphore {
	tickets := make(chan struct{}, ticketsCount)
	for i := 0; i < ticketsCount; i++ {
		tickets <- struct{}{}
	}
	return &Semaphore{
		tickets: tickets,
	}
}

func (s *Semaphore) Acquire() {
	<-s.tickets
}

func (s *Semaphore) Release() {
	s.tickets <- struct{}{}
}
