deps:
	go get github.com/tools/godep && godep restore

test:
	go test -v ./...
