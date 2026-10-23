FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/xray ./cmd/xray \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mockrelay ./cmd/mockrelay

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/ /usr/local/bin/
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/xray"]
