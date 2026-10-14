FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go test ./... && CGO_ENABLED=0 go build -o /gcsproxy .

FROM gcr.io/distroless/static-debian12
COPY --from=build /gcsproxy /gcsproxy
ENTRYPOINT ["/gcsproxy"]
CMD ["-b", "0.0.0.0:8080"]
