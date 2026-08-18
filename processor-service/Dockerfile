FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY sentimentpb/ ./sentimentpb/
COPY processor-service/ ./processor-service/
RUN go build -o /processor-service ./processor-service

FROM alpine:3.20
COPY --from=build /processor-service /processor-service
ENTRYPOINT ["/processor-service"]