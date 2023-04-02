darwin_amd: clean
	mkdir -p bin
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -mod=vendor -o ./bin/pushninja ./app/*.go

clean:
	rm -rf bin