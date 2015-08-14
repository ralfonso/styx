FROM golang
COPY . /styx
RUN rm -rf /styx/bin /styx/pkg && \
    go get github.com/constabulary/gb/... && \
    cd /styx && \
    gb vendor restore && \
    gb build all
ENV DEBUG=1
CMD /styx/bin/styx
