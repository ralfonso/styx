FROM golang
EXPOSE 6389
ADD . /go/src/styx
WORKDIR /go/src/styx
#ENV DEBUG=1
ENV GO15VENDOREXPERIMENT=1
CMD go get github.com/Masterminds/glide && glide install && go install styx && styx
