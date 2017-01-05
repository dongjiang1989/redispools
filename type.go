package Redis

type Item struct {
	Key   string
	Rtype string // set\ hash\ string
}

type SetItem struct {
	Key string
}

func (r *SetItem) Rtype() string {
	return "set"
}

type HashItem struct {
	Key string
}

func (r *HashItem) Rtype() string {
	return "hash"
}

type StringItem struct {
	Key string
}

func (r *StringItem) Rtype() string {
	return "string"
}
