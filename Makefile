compile:
	protoc api/log.v1/*.proto --go_out=plugins=grpc:.

TAG ?= 0.0.1
build-docker:
	docker build - github/demas/distributed-services:$(TAG) .