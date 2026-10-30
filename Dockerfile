# vectra-server container image.
#
# Build from the repository root (NOT from server/): server/go.mod contains
# `replace github.com/projanvil/vectra => ../`, so the build context must
# include the root module:
#
#   docker build -t vectra-server:latest .
#
# No HEALTHCHECK is defined: the runtime stage is FROM scratch and has no
# shell, so no probe command can run inside the container. Check liveness
# from outside with GET /v1/health on the HTTP port (8080).
#
# /data is created in the image and pre-owned by uid 65534 so the
# anonymous volume Docker mounts over it inherits writable ownership
# (copy-up preserves image ownership); the first real docker build/run
# must still confirm this, as the local Docker daemon was down when this
# recipe was written.

FROM golang:1.27-alpine AS build
WORKDIR /src

# Module manifests first so dependency download stays cached across
# source-only edits.
COPY go.mod go.work ./
COPY server/go.mod server/go.mod
COPY mcp/go.mod mcp/go.mod

# Third-party dependencies only: the replace directive keeps the Vectra
# engine module local; it arrives with COPY . . below.
RUN cd server && go mod download

COPY . .

RUN cd server && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/vectra-server ./cmd/vectra-server

# Data directory for the runtime stage. It must exist with the runtime
# user's ownership so the volume copy-up in stage 2 hands the anonymous
# volume writable ownership instead of a root-owned 0755 mountpoint.
RUN mkdir /data && chown 65534:65534 /data

FROM scratch
COPY --from=build /out/vectra-server /vectra-server
COPY --from=build --chown=65534:65534 /data /data
VOLUME /data
EXPOSE 8080 9000
USER 65534:65534
ENTRYPOINT ["/vectra-server"]
CMD ["-id","n1","-data","/data","-http",":8080","-raft",":9000"]
