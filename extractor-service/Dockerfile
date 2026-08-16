FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY extractor-service/ ./extractor-service/
RUN go build -o /extractor-service ./extractor-service

FROM alpine:3.20
COPY --from=build /extractor-service /extractor-service
COPY data/ /data/
ENTRYPOINT ["/extractor-service"]