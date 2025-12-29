# .agentdocs 索引

本目录仅用于 AI 代理记录项目技术治理信息、架构边界与长期可复用的约束/共识，不面向人类说明。

## 后端文档
`backend/architecture.md` - 后端目录分层、鉴权链路、API 增改入口与改动清单

## 当前任务文档
`workflow/251224-integrate-lessonin-coze.md` - lessonin-admin 作为主站，Coze 作为创作中心嵌入，统一鉴权/校验与路由

## 全局重要记忆
- 后端为 Go（`backend/go.mod`），HTTP 框架使用 CloudWeGo Hertz（`backend/main.go`），API 合同使用 Thrift IDL（`idl/`），路由/模型/部分 handler 由生成器产出（`backend/api/router/`、`backend/api/model/`、`backend/api/handler/`）；Hertz 生成器配置文件为 `backend/.hz`（`hz version: v0.9.7`）。
- 鉴权分为两类：Web API（cookie session，默认 `/api/**`）与 OpenAPI（`Authorization: Bearer <token>`，主要 `/v1/**`、`/v3/**`），由 `backend/api/middleware/request_inspector.go` 判定类型并由中间件链路执行。
