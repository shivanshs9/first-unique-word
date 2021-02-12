package main

import (
	"container/list"
)

func popFront(l *list.List) interface{} {
	e := l.Front()
	if e == nil {
		return nil
	}
	return l.Remove(e)
}

func removeDuplicates(sourceWords *list.List, nextWords []string) {
	dupWords := make(WordHashSet)
	for _, word := range nextWords {
		dupWords.Add(word)
	}
	// modList := list.New()
	// modList.PushBackList(sourceWords)
	// sourceWords.Init()
	remElems := sourceWords.Len()
	for word := popFront(sourceWords); remElems > 0; word = popFront(sourceWords) {
		if !dupWords.Has(word.(string)) {
			sourceWords.PushBack(word)
		}
		remElems--
	}
}

func getOnlyUnique(words []string) *list.List {
	freqWords := make(map[string]int, len(words))
	for _, word := range words {
		freqWords[word]++
	}
	wordsList := list.New()
	for word, freq := range freqWords {
		if freq == 1 {
			wordsList.PushBack(word)
		}
	}
	return wordsList
}
