# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/lang/zh-CN/).

## [Unreleased]

### Added

## [1.1.0] - 2026-06-15

### Added
- RPC 模式（`Call` / `ServeRPC` + `RPCHandler` / `RPCCallPublisher` / `RPCServer` 接口），支持 simple/direct/topic/headers
- Headers Exchange 模式支持（`NewHeaders` / `NewPubHeaders` / `NewConsumeHeaders`）
- `WithQueueArgs` / `WithExchangeArg` Option，自定义 queue/exchange 声明参数
- `BatchPublish` 批量发布方法
- `routedMQ`（MQDirect / MQTopic）和 MQFanout 增加 `PublishWithDlxString` / `PublishWithDlx` 方法
- TLS 连接日志警告（`WithTLSConfig` + `amqp://` 时输出 Warning）

## [1.0.0] - 2026-06-15

### Added
- 四种消息模式：Simple、Direct、Fanout、Topic
- 消费端自动重连（指数退避：1s → 2s → 4s → ... → 30s）
- 发布端复用 confirm channel（`Publisher Confirm`）
- 延迟消息（TTL + Dead Letter 实现）
- 失败重试机制（retry queue + `x-retry` header）
- 死信拓扑（DLX + DLQ）的一键声明与消费
- 手动重试 API（`Retrier` 接口）
- 全局 + 实例级 Logger 注入（`SetLogger` / `WithLogger`）
- 多种外部 Logger 自动适配（`adaptedLogger`）
- 可观测性 Observer 钩子（`OnPublish` / `OnConsume` / `OnReconnect`）
- 协议层永久错误快速失败（`ErrPermanent`）
- 错误哨兵体系（`ErrDestroyed` / `ErrPublishNotAcknowledged` / `ErrConnectionNotInitialized` / `ErrHandlerRequired` / `ErrNotInitialized` / `ErrPermanent`）
- `Destroy()` 生命周期的 in-flight publish 安全等待
- Functional Options 配置体系（16 个 Option + `NewConfig` 配置复用）
- TLS / Vhost / Heartbeat / PrefetchCount 配置支持
- `Publisher` / `Consumer` / `MQInterface` / `Retrier` 细粒度接口隔离
- 测试桩和 mock 辅助
- 8 个可直接运行的 example 程序
