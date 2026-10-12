// Package server is Vectra's optional server module: hashicorp/raft replication +
// HTTP API + hot-swap publishing. The core engine module stays dependency-free;
// this module's dependency tree only affects users choosing the server deployment.
package server
