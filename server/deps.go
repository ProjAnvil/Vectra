// Package server 是 Vectra 的可选服务模块：hashicorp/raft 复制 +
// HTTP API + 热替换发布。核心引擎模块保持零依赖；本模块的依赖树
// 只影响选择部署服务形态的用户。
package server
