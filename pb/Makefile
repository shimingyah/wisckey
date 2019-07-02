regenerate:
	# you might need to go get github.com/gogo/protobuf/protoc-gen-gofast
	(protoc -I=. -I=$(GOPATH)/src/github.com/gogo/protobuf/protobuf -I=$(GOPATH)/src --gofast_out=plugins=grpc:. pb.proto)