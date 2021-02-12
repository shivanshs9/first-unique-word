package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"

	bytesize "github.com/inhies/go-bytesize"
)

const MaxMemLimit = 10 * bytesize.MB
const AvgWordLength = 10

var SizeReadBuffer = bytesize.New(0.4 * float64(MaxMemLimit))
var SizeLRUCache = bytesize.New(0.2 * float64(MaxMemLimit))
var NumWorkers = int((MaxMemLimit-SizeReadBuffer)/SizeLRUCache) - 1
var WordsPerWorker = int(SizeLRUCache) / AvgWordLength

type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

type wordProcessor struct {
	numPartitions int
	partitions    []int64
}

/*
	Reads the string, exclusive of the last character
*/
func trimContentToWords(input string, noChangeLength bool) (output []string, diff int) {
	output = strings.Fields(input)
	diff = 0
	if noChangeLength {
		return
	}
	strLen := len(input)
	splitLen := len(output)
	if input[strLen-1] != ' ' {
		diff = -len(output[splitLen-1])
		output = output[:splitLen-1]
	}
	return
}

func (proc *wordProcessor) getWordsPartition(partitionIdx int, reader ReadSeekCloser, buffer []byte) ([]string, error) {
	startByte := int64(0)
	if partitionIdx > 0 {
		startByte = proc.partitions[partitionIdx-1]
	}
	endByte := proc.partitions[partitionIdx]
	reader.Seek(startByte, io.SeekStart)
	n, err := reader.Read(buffer)
	var isEOF bool
	if err != nil {
		if err == io.EOF {
			isEOF = true
			if n == 0 {
				log.Println("Read complete")
				return nil, err
			}
		} else {
			log.Fatal(err)
		}
	}
	var input string
	if endByte != 0 && endByte < int64(n) {
		input = string(buffer[:endByte])
	} else {
		input = string(buffer[:n])
	}
	words, diff := trimContentToWords(input, isEOF || endByte != 0)
	if endByte == 0 {
		proc.partitions[partitionIdx] = startByte + int64(n+diff)
	}
	if diff != 0 {
		log.Printf("Backtrack from %d to %d\n", startByte+int64(n), proc.partitions[partitionIdx])
	}
	return words, nil
}

func (proc *wordProcessor) readFromStream(reader ReadSeekCloser) (result string) {
	defer func() {
		log.Println("Ended read goroutine")
		reader.Close()
	}()
	buffer := make([]byte, int(SizeReadBuffer))
	for sourcePartition := 1; sourcePartition <= proc.numPartitions; sourcePartition++ {
		log.Println("Source Partition: ", sourcePartition)
		sourceWords, err := proc.getWordsPartition(sourcePartition, reader, buffer)
		if err == io.EOF {
			break
		}
		wordsList := getOnlyUnique(sourceWords)
		for nextPartition := sourcePartition + 1; nextPartition <= proc.numPartitions; nextPartition++ {
			if nextPartition == sourcePartition {
				continue
			}
			log.Println("Next Partition: ", nextPartition)
			if wordsList.Len() < 1 {
				break
			}
			nextWords, err := proc.getWordsPartition(nextPartition, reader, buffer)
			if err == io.EOF {
				break
			}
			removeDuplicates(wordsList, nextWords)
		}
		log.Println(wordsList.Len())
		if wordsList.Len() > 0 {
			result = wordsList.Front().Value.(string)
			break
		}
	}
	return
}

func (proc *wordProcessor) findUniqueFromStream(reader ReadSeekCloser) string {
	result := proc.readFromStream(reader)
	if result == "" {
		return "None found"
	}
	return result
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
	numPartitions := int(math.Ceil(float64(reader.Size) / float64(SizeReadBuffer)))
	proc := &wordProcessor{
		numPartitions: numPartitions,
		partitions:    make([]int64, numPartitions+1),
	}
	result := proc.findUniqueFromStream(reader)
	log.Printf("Result: %s\n", result)
}
