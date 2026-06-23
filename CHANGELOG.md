# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/lang/zh-CN/).

## [Unreleased]

### Added
- Quorum 队列支持：`WithQueueType(QueueTypeQuorum)`，用于 RabbitMQ 4.x 下的队列高可用（替代已移除的 classic 镜像队列），需 broker 3.8+
- Stream 队列支持：`NewStream` / `NewPubStream` / `NewConsumeStream` + `MQStream` + `StreamOffset`（`OffsetFirst` / `OffsetLast` / `OffsetNext`），适合大数据量 / 消息重放，需 broker 3.9+
- `WithPriority` Option：把队列 `x-max-priority` 从无条件强加改为可选（classic 默认仍为 10，传 0 关闭；quorum/stream 始终不写）
- `WithDeliveryLimit` Option：为 quorum 队列设置 `x-delivery-limit`（原生毒消息处理）
- `WithDelayedExchange` Option：启用 `rabbitmq_delayed_message_exchange` 插件实现延迟投递（单 exchange 承载混合 TTL，无队头阻塞）
- `WithPublisherConnection` Option：发布与消费使用独立 connection，避免 TCP 背压互相影响
- 可选 `BlockObserver` 接口 + `BlockedEvent`：感知 broker 因内存/磁盘告警阻塞/恢复连接（不破坏既有 `Observer` 实现者）
- `ErrPublishReturned` 错误哨兵：mandatory 发布的消息不可路由被 broker 退回时返回

### Changed
- 延迟消息默认改为按 TTL 分桶到独立 delay 队列（队列名含 TTL），不同 TTL 不再互相队头阻塞

### Fixed
- 修复 mandatory 发布消息不可路由时被静默丢弃、`Publish` 却返回成功的问题：现注册 `NotifyReturn`，退回的消息使 `Publish`/`BatchPublish` 返回 `ErrPublishReturned` 并触发 `PubFailNotify`
- 统一 direct/topic 普通 `Publish` 的 mandatory 语义，与 simple/fanout 一致
- 修复 headers 模式 `PublishDelay` 经业务 exchange 投递导致延迟消息路由不到 delay 队列的问题（改走默认 exchange）

### Migration
- 默认行为（classic 队列、声明参数）与 v1.1.0 完全一致，存量用户升级无需改代码
- 启用 quorum：对**已存在的同名 classic 队列**改用 quorum 会被 broker 以 `PRECONDITION_FAILED` 拒绝（队列类型不可原地变更），需先删除或更换队列名
- mandatory 行为修正后，此前路由错配被静默"成功"的发布现在会返回 `ErrPublishReturned`，请检查相关错误处理

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
