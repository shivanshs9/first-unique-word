package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"

	bytesize "github.com/inhies/go-bytesize"
	"github.com/pkg/profile"
)

const MaxMemLimit = 16 * bytesize.GB

var SizeReadBuffer = bytesize.New(0.4 * float64(MaxMemLimit))

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
	if endByte != 0 && (endByte-startByte) < int64(n) {
		input = string(buffer[:endByte-startByte])
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
		for nextPartition := 1; nextPartition <= proc.numPartitions; nextPartition++ {
			if nextPartition == sourcePartition {
				continue
			}
			log.Println("Next Partition: ", nextPartition)
			if wordsList.Len() < 1 {
				log.Println("list emptied")
				break
			}
			nextWords, err := proc.getWordsPartition(nextPartition, reader, buffer)
			if err == io.EOF {
				break
			}
			removeDuplicates(wordsList, nextWords)
		}
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

func calculatePartitions(fileSize bytesize.ByteSize) int {
	return int(math.Ceil(float64(fileSize)/float64(SizeReadBuffer))) + 1
}

func main() {
	defer profile.Start(profile.MemProfile).Stop()
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
	numPartitions := calculatePartitions(reader.Size)
	proc := &wordProcessor{
		numPartitions: numPartitions,
		partitions:    make([]int64, numPartitions+1),
	}
	result := proc.findUniqueFromStream(reader)
	log.Printf("Result: %s\n", result)

	// runtime.GC()
	// memProfile, err := os.Create(fmt.Sprintf("%s.prof", inputFile))
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer memProfile.Close()
	// if err := pprof.WriteHeapProfile(memProfile); err != nil {
	// 	log.Fatal(err)
	// }
}
