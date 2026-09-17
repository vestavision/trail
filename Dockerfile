FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG COMMAND
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./cmd/${COMMAND}
FROM alpine:3.22
RUN adduser -D -u 10001 trail
COPY --from=build /out/app /usr/local/bin/trail-app
COPY --chmod=0555 docker-entrypoint.sh /usr/local/bin/trail-entrypoint
COPY --chown=trail:trail startup-banner.txt /etc/trail/startup-banner.txt
USER trail
ENTRYPOINT ["/usr/local/bin/trail-entrypoint"]
