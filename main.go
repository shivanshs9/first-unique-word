package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	bytesize "github.com/inhies/go-bytesize"
)

const MaxMemLimit = 10 * bytesize.MB
const AvgWordLength = 10

var SizeReadBuffer = bytesize.New(0.4 * float64(MaxMemLimit))
var SizeLRUCache = bytesize.New(0.2 * float64(MaxMemLimit))
var NumWorkers = int((MaxMemLimit-SizeReadBuffer)/SizeLRUCache) - 1
var WordsPerWorker = int(SizeLRUCache) / AvgWordLength

var workerLog = log.New(os.Stdout, "[WORKER] ", log.LstdFlags)

type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

type wordProcessor struct {
	wordStreams []chan input
	wg          sync.WaitGroup
	results     chan output
	doneCtr     int
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

func (proc *wordProcessor) Close() {
	for _, stream := range proc.wordStreams {
		close(stream)
	}
}

func (proc *wordProcessor) readFromStream(reader ReadSeekCloser) {
	defer func() {
		log.Println("Ended read goroutine")
		proc.Close()
		reader.Close()
	}()
	buffer := make([]byte, int(SizeReadBuffer))
	isEOF := false
	ctr := 0
	for {
		n, err := reader.Read(buffer)
		if err != nil {
			if err == io.EOF {
				log.Println("Read complete")
				isEOF = true
			} else {
				log.Fatal(err)
			}
		}
		words, newLen := trimContentToWords(string(buffer[:n]), isEOF)
		for _, word := range words {
			if ctr >= WordsPerWorker {
				ctr = 0
				proc.doneCtr++
				workerLog.Println("Moving to next worker: ", proc.doneCtr%NumWorkers)
			}
			proc.wordStreams[proc.doneCtr%NumWorkers] <- input{
				word:      word,
				partition: proc.doneCtr,
			}
			ctr++
		}
		if !isEOF && newLen != n {
			log.Printf("Backtrack from %d to %d\n", n, newLen)
			_, err = reader.Seek(int64(newLen-n), io.SeekCurrent)
			if err != nil {
				log.Fatal(err)
			}
		}
		if isEOF {
			break
		}
	}
}

func findUniqueFromStream(reader ReadSeekCloser) string {
	proc := &wordProcessor{
		wordStreams: make([]chan input, NumWorkers),
		results:     make(chan output),
	}
	go func() {
		for i := 0; i < NumWorkers; i++ {
			proc.wordStreams[i] = make(chan input, WordsPerWorker)
			proc.wg.Add(1)
			go proc.processWordsWorker(i)
		}
		proc.wg.Wait()
		close(proc.results)
	}()

	result := proc.getUniqueResult()
	go proc.readFromStream(reader)
	return (<-result).word
}

func main() {
	log.SetPrefix("[MAIN] ")
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
	log.Printf("Result: %s\n", result)
}
