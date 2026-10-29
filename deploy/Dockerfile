FROM golang:1.26.5 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/stockio ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/stockio /stockio
EXPOSE 8080
ENTRYPOINT ["/stockio"]
