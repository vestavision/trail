FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG COMMAND
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./cmd/${COMMAND}
FROM alpine:3.22
RUN adduser -D -u 10001 trail
USER trail
COPY --from=build /out/app /usr/local/bin/trail-app
ENTRYPOINT ["/usr/local/bin/trail-app"]
