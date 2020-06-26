package util

type Set map[int]bool

func (s Set) Contain(i int) bool {
	_, ok := s[i]
	return ok
}

func (s Set) Add(i int) bool {
	if s.Contain(i) {
		return false
	}
	s[i] = true
	return true
}

func (s Set) Delete(i int) {
	delete(s, i)
}

func (s Set) Empty() bool {
	r := false
	for _, b := range s {
		r = r || b
	}
	return !r
}

func NewSet() Set {
	return make(map[int]bool)
}
