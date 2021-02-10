package main

import (
	"container/list"
	"sort"
)

type input struct {
	word      string
	partition int
}

type output = input

const NO_PARTITION = -1

func findUniqueWords(inputStream <-chan input, outputStream chan<- output, onlyFirstUnique bool) {
	lruQueue := list.New()
	wordFreq := make(WordHashSet, WordsPerWorker)
	lastPartition := NO_PARTITION
	for elem := range inputStream {
		if lastPartition != NO_PARTITION && elem.partition != lastPartition {
			if onlyFirstUnique {
				outputStream <- lruQueue.Front().Value.(output)
			} else {
				for e := lruQueue.Front(); e != nil; e = e.Next() {
					outputStream <- e.Value.(output)
				}
			}

			// Clearing for new data
			lruQueue.Init()
			wordFreq = make(WordHashSet, WordsPerWorker)
		}
		if wordFreq.Has(elem.word) {
			for e := lruQueue.Front(); e != nil; e = e.Next() {
				if (e.Value.(input)).word == elem.word {
					lruQueue.Remove(e)
					break
				}
			}
		} else {
			wordFreq.Add(elem.word)
			lruQueue.PushBack(elem)
		}
		lastPartition = elem.partition
	}
	if onlyFirstUnique {
		outputStream <- lruQueue.Front().Value.(output)
	} else {
		for e := lruQueue.Front(); e != nil; e = e.Next() {
			outputStream <- e.Value.(output)
		}
	}
}

func pushToPartitionChannel(inputs []input, c chan<- input, partition int) {
	sort.Slice(inputs, func(i, j int) bool {
		return inputs[i].partition < inputs[j].partition
	})
	for _, elem := range inputs {
		c <- input{
			word:      elem.word,
			partition: partition,
		}
	}
}

func (proc *wordProcessor) processWordsWorker(workerID int) {
	defer proc.wg.Done()
	findUniqueWords(proc.wordStreams[workerID], proc.results, false)
}

func (proc *wordProcessor) getUniqueResult() chan output {
	result := make(chan output)
	go func() {
		partitionStream := make(chan input, WordsPerWorker)
		results := make(chan output)
		go func() {
			defer close(results)
			findUniqueWords(partitionStream, results, false)
		}()

		ctr := 0
		partition := 0
		newInputs := make([]input, 0)
		for elem := range proc.results {
			workerLog.Printf("Obtained partition %d, word %s", elem.partition, elem.word)
			if ctr >= WordsPerWorker {
				pushToPartitionChannel(newInputs, partitionStream, partition)

				newInputs = nil
				ctr = 0
				partition++
			}
			newInputs = append(newInputs, elem)
			ctr++
		}
		pushToPartitionChannel(newInputs, partitionStream, partition)
		newInputs = nil
		close(partitionStream)

		partitionStream = make(chan input)
		go func() {
			defer close(result)
			findUniqueWords(partitionStream, result, true)
		}()
		for elem := range results {
			partitionStream <- input{
				word:      elem.word,
				partition: 0,
			}
		}
	}()
	return result
}
