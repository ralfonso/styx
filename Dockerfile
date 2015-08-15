FROM golang
RUN go get github.com/constabulary/gb/...
#ENV DEBUG=1
#CMD /styx/bin/styx
CMD /bin/bash
