FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY sink-service/ ./sink-service/
RUN go build -o /sink-service ./sink-service

FROM alpine:3.20
COPY --from=build /sink-service /sink-service
ENTRYPOINT ["/sink-service"]