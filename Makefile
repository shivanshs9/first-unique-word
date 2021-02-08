OUTPUT_BIN := ./build/program
SOURCE_CODE := *.go

run: build
	$(OUTPUT_BIN) $(ARGS)

build: $(OUTPUT_BIN)

$(OUTPUT_BIN): $(SOURCE_CODE)
	go build -o $(OUTPUT_BIN) $(SOURCE_CODE)
