FROM golang:1.26.10-bookworm@sha256:23ce71337b726608afb1bc00986e630a05034a87a87ea1fc2cd1ae5cf33387eb AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go test ./... && CGO_ENABLED=0 go build -o /gcsproxy .

FROM cgr.dev/chainguard/wolfi-base@sha256:629d9acaceda7746cd9293365eb0d8e8fabf74e91830b05470cfbee20b53f447
RUN apk upgrade --no-cache
COPY --from=build /gcsproxy /gcsproxy
ENTRYPOINT ["/gcsproxy"]
CMD ["-b", "0.0.0.0:8080"]
