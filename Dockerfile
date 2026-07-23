FROM golang:1.26.2-alpine@sha256:f85330846cde1e57ca9ec309382da3b8e6ae3ab943d2739500e08c86393a21b1 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG PACKAGE
ARG TAGS
RUN CGO_ENABLED=0 go build -trimpath -tags "${TAGS}" -o /out/service "${PACKAGE}"

FROM alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40
RUN addgroup -S service && adduser -S -G service service
COPY --from=build /out/service /usr/local/bin/service
USER service
ENTRYPOINT ["/usr/local/bin/service"]
