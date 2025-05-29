FROM golang:1.24

WORKDIR /app
COPY src .
RUN go mod download
RUN go build -v

CMD ["/app/mqtt2sqlite", "-h", "tcp://mqtt:1883", "-s", "dbdata"]
