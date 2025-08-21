
# Image URL to use all building/pushing image targets
VERSION=latest
IMG ?= docker.io/cinaq/pv-zone-operator:$(VERSION)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

all: test docker-build

# Run tests
test: fmt vet
	go test -v ./...

# Run against the configured Kubernetes cluster in ~/.kube/config
run: generate fmt vet manifests
	go run ./main.go

# Run go fmt against code
fmt:
	go fmt ./...

# Run go vet against code
vet:
	go vet ./...

# Build the docker image
docker-build:
	docker build . -t ${IMG}

# Push the docker image
docker-push:
	docker push ${IMG}
