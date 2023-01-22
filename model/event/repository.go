package event

type Repository interface {
	Store(Event) error
}
