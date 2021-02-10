package main

import (
	"io"
	"log"
	"os"

	"github.com/inhies/go-bytesize"
)

type fileReader struct {
	fileName string

	filePtr *os.File
	Size    bytesize.ByteSize

	bytesRead int64
}

func ReadFromFile(fileName string) (*fileReader, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &fileReader{
		fileName: fileName,
		filePtr:  file,
		Size:     bytesize.New(float64(fi.Size())),
	}, nil
}

// Read satisfies io.Reader
// It's necessary to encounter both EOF and non-zero read bytes at the same time
// when file has been read completely
func (reader *fileReader) Read(buffer []byte) (read int, err error) {
	read, err = reader.filePtr.Read(buffer)
	if read != 0 {
		reader.bytesRead += int64(read)
		log.Printf("%.2f%% complete: Read %s\n", float64(reader.bytesRead*100)/float64(reader.Size), bytesize.New(float64(reader.bytesRead)))
	}
	if err == nil && reader.bytesRead == int64(reader.Size) {
		err = io.EOF
	}
	return
}

func (reader *fileReader) Seek(offset int64, whence int) (int64, error) {
	if whence == 1 {
		reader.bytesRead += offset
	}
	return reader.filePtr.Seek(offset, whence)
}

func (reader fileReader) Close() error {
	return reader.filePtr.Close()
}
