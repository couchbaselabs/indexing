genproto:
	protoc --go_out=. *.proto

bench:
	@protoc --go_out=. *.proto
	@go test -test.bench=.
