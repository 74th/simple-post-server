FROM golang:1.16 AS build-env
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN go build -o server main.go
FROM debian
WORKDIR /app

FROM debian AS app
WORKDIR /app
COPY --from=build-env /app/server ./
EXPOSE 8080
ENTRYPOINT [ "/app/server" ]
