package processor

type Converter interface {
	Convert(any) (any, error)
}
