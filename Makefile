gb := vendor/bin/gb

.PHONY: build test

build: $(gb)
	$(gb) build all

test: $(gb)
	$(gb) test all

$(gb):
	bash -c 'GOPATH="$$(pwd)/vendor" go get github.com/constabulary/gb/...'
