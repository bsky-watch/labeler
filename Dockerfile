FROM golang:1.22.3 as builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN for cmd in copy labeler list-labeler update-plc; do go build -trimpath ./cmd/${cmd}; done

FROM alpine:latest as certs
RUN apk --update add ca-certificates

FROM debian:stable-slim
VOLUME /data
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /app/copy /app/labeler /app/list-labeler /app/update-plc .
ENTRYPOINT ["./labeler"]
