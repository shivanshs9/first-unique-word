package main

import (
	"container/list"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	bytesize "github.com/inhies/go-bytesize"
)

const MaxMemLimit = 10 * bytesize.MB

var SizeReadBuffer = bytesize.New(0.4 * float64(MaxMemLimit))
var SizeLRUCache = bytesize.New(0.4 * float64(MaxMemLimit))

type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

/*
	Reads the string, exclusive of the last character
*/
func trimContentToWords(input string, isEOF bool) (output []string, length int) {
	strLen := len(input)
	output = strings.Fields(input)
	splitLen := len(output)
	diff := 0
	if isEOF {
		length = strLen
		return
	}
	if input[strLen-2] != ' ' {
		if input[strLen-1] != ' ' {
			diff = len(output[splitLen-1])
			output = output[:splitLen-1]
		}
	} else if input[strLen-1] != ' ' {
		diff = len(output[splitLen-1])
		output = output[:splitLen-1]
	}
	length = strLen - diff
	return
}

func processWords(wordStream <-chan string, result chan<- string) {
	lruQueue := list.New()
	wordFreq := WordHashSet{}
	for word := range wordStream {
		if wordFreq.Has(word) {
			for e := lruQueue.Front(); e != nil; e = e.Next() {
				if e.Value == word {
					lruQueue.Remove(e)
					break
				}
			}
		} else {
			wordFreq.Add(word)
			lruQueue.PushBack(word)
		}
	}
	result <- lruQueue.Front().Value.(string)
}

func findUniqueFromStream(reader ReadSeekCloser) string {
	buffer := make([]byte, int(SizeReadBuffer))
	isEOF := false

	wordStream := make(chan string, int(SizeLRUCache))
	result := make(chan string)
	go processWords(wordStream, result)

	for {
		n, err := reader.Read(buffer)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Read complete")
				isEOF = true
			} else {
				log.Fatal(err)
			}
		}
		words, newLen := trimContentToWords(string(buffer[:n]), isEOF)
		for _, word := range words {
			wordStream <- word
		}
		if !isEOF && newLen != n {
			fmt.Printf("Backtrack from %d to %d\n", n, newLen)
			_, err = reader.Seek(int64(newLen-n), io.SeekCurrent)
			if err != nil {
				log.Fatal(err)
			}
		}
		if isEOF {
			fmt.Println("Ended read goroutine")
			close(wordStream)
			reader.Close()
			break
		}
	}
	return <-result
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Invalid use -- Provide a filename")
	}
	inputFile := os.Args[1]
	reader, err := ReadFromFile(inputFile)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("File size: %s\n", reader.Size)
	result := findUniqueFromStream(reader)
	fmt.Printf("Result: %s\n", result)
}
