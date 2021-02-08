package main

type WordHashSet map[string]struct{}

func (set WordHashSet) Add(word string) {
	set[word] = struct{}{}
}

func (set WordHashSet) Remove(word string) {
	delete(set, word)
}

func (set WordHashSet) Has(word string) bool {
	_, ok := set[word]
	return ok
}
