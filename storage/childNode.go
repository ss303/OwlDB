package storage

type IChildNode interface {
	GetChild(string) (IChildNode, error)
	Handle(RequestPack) (content any, status status)
	GetPath() string
}
