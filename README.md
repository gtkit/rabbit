# mq

`mq` 是一个基于 `github.com/rabbitmq/amqp091-go` 的 RabbitMQ Go 封装，支持以下四种常见消息模型：

- `simple`：普通队列
- `direct`：直连交换机
- `fanout`：广播交换机
- `topic`：主题交换机

项目当前最低需要 `Go 1.22`（使用了 `range over int`、`maps.Copy` 等现代特性）。

本文档会尽量把库里已经提供的能力、每个方法的用途、典型使用方式、重试与死信行为、日志注入方式都说明清楚。

## 一、特性概览

- 支持 `simple`、`direct`、`fanout`、`topic` 四种 RabbitMQ 模式
- 消费端支持自动重连（连接 + 通道双层重连，指数退避）
- 发布端复用 confirm channel，避免每条消息都重新打开 channel
- 发布时对 exchange / queue / retry queue / delay queue 做 channel 级声明缓存
- 支持延迟消息，基于 `TTL + Dead Letter` 实现，不依赖 `x-delayed-message` 插件
- 支持失败重试，使用消息头 `x-retry` 记录当前重试次数
- 支持死信消费
- 公共消费循环统一抽到 `runtime.go`，四种模式只声明拓扑差异
- 消费者 `handler.Process` 在 `recover()` 保护下调用，panic 不会杀掉消费 goroutine
- 全局 + 实例级 Logger 注入
- `Destroy()` 可重复调用，并会安全关闭内部资源

## 二、安装

```bash
go get github.com/gtkit/mq
```

依赖：

```bash
go get github.com/rabbitmq/amqp091-go
go get github.com/google/uuid
```

## 三、目录说明

核心代码在 `rabbit/` 目录下。

主要文件：

- `rabbit/simple.go`：simple 模式
- `rabbit/direct.go`：direct 模式
- `rabbit/fanout.go`：fanout 模式
- `rabbit/topic.go`：topic 模式
- `rabbit/config.go`：Option 配置
- `rabbit/log.go`：日志接口与注入
- `rabbit/mq.go`：公共结构、生命周期、`MsgHandler` / `FailedMsg`
- `rabbit/runtime.go`：公共消费循环、重连、重试

## 四、基础概念

### 1. `MsgHandler`

消费业务通过 `MsgHandler` 注入：

```go
type MsgHandler interface {
    Process(body []byte, msgID string) error
    Failed(FailedMsg)
}
```

- `Process(body []byte, msgID string) error`
  - 返回 `nil`：消费成功，自动 `Ack`
  - 返回 `error`：触发 retry 或 reject（取决于消费模式）
  - 内部 `recover()` 保护，panic 会被转成 error
- `Failed(msg FailedMsg)`
  - 在消息最终失败（重试耗尽 / DLX 处理失败 / ctx 取消）时被调用
  - 如果不需要可以提供空实现

### 2. `FailedMsg`

失败回调结构：

```go
type FailedMsg struct {
    ExchangeName string
    QueueName    string
    RoutingKey   string
    MessageID    string
    Message      []byte
}
```

### 3. 发布失败回调

发布端没有 `handler` 形参，发布失败时通过 `WithPubFailNotify` 注册的回调通知：

```go
rabbit.WithPubFailNotify(func(msg rabbit.FailedMsg) {
    log.Printf("publish failed: msgID=%s body=%s", msg.MessageID, msg.Message)
})
```

### 4. 生命周期

每个 `MQSimple` / `MQDirect` / `MQFanout` / `MQTopic` 实例内部维护：

- 一个 RabbitMQ connection
- 消费时临时创建的 consumer channel
- 一个复用的 publish channel（confirm 模式）
- 一个受内部管理的 context

实例使用结束后调用：

```go
defer mq.Destroy()
```

`Destroy()` 行为：取消内部 context、关闭 publish channel、关闭 connection；可重复调用。

## 五、通用配置项

所有构造函数都支持 `opts ...Option`。

### `WithContext(ctx context.Context)`

控制生命周期；`ctx.Done()` 后消费循环退出、发布提前返回。

### `WithConnectionName(name string)`

设置 connection name，便于在 RabbitMQ 管理后台识别来源。

### `WithQueueName(name string)`

显式指定消费使用的队列名（对 `direct` / `fanout` / `topic` 模式建议固定）。

### `WithMaxRetry(maxRetry int32)`

设置消费失败时的最大重试次数。默认 `3`。仅对 `simple` / `direct` / `topic` 生效；`fanout` 不做自动 retry。

### `WithRetryTTL(ttl time.Duration)`

设置失败重试消息在 retry queue 中的停留时长。默认 `2s`。

> v1.1.0 起类型改为 `time.Duration`，不再是毫秒字符串。

### `WithLogger(l Logger)`

为当前实例注入 logger，覆盖全局 logger（用于多业务隔离日志）。

### `WithPubFailNotify(fn func(FailedMsg))`

设置发布失败回调（发布侧版本的 `MsgHandler.Failed`）。

### `WithObserver(o Observer)`

注入观测钩子，用于接入 Prometheus / OpenTelemetry。`Observer` 接口：

```go
type Observer interface {
    OnPublish(event PublishEvent)
    OnConsume(event ConsumeEvent)
    OnReconnect(event ReconnectEvent)
}
```

事件结构包含 Mode / Operation / 拓扑信息 / MessageID / BodySize / Duration / Err / Retry。回调内部由库自动 `recover()`，业务不必再保护。

### `WithVhost(vhost string)`

设置 AMQP vhost；空串等价默认 `/`。

### `WithHeartbeat(d time.Duration)`

设置 AMQP 心跳周期；`<=0` 回退默认 10s。

### `WithPrefetchCount(n int)`

设置消费端预取数；`<=0` 回退默认 1。高吞吐场景调高（如 50/100）能显著提速。

### `WithTLSConfig(c *tls.Config)`

注入 TLS 配置。注意 `mqURL` 仍按业务给定的 scheme 决定（`amqp://` 或 `amqps://`）。

## 六、日志注入

库内部定义的最小日志接口：

```go
type Logger interface {
    Info(args ...any)
    Infof(template string, args ...any)
    Errorf(template string, args ...any)
}
```

### 1. 全局注入 `SetLogger(l any) bool`

支持两类输入，匹配失败返回 `false`：

- 已实现 `Logger` 接口的对象，直接使用
- 只实现了 `Infof(template string, args ...any)` 和 `Errorf(template string, args ...any)` 的对象，自动适配

示例：

```go
import gtlogger "github.com/gtkit/logger"

if !rabbit.SetLogger(gtlogger.Default()) {
    panic("inject gtkit logger failed")
}
```

> v1.1.0 起 `SetExternalLogger` 已合并到 `SetLogger`，原函数被移除。

### 2. 实例级 `WithLogger`

```go
mq, err := rabbit.NewPubSimple(
    "demo.queue", mqURL,
    rabbit.WithLogger(myLogger),
)
```

实例 logger 优先于全局 logger。

### 3. 不注入 logger 时的默认行为

默认 `Log` 实现：`Infof` / `Info` 加 `[INFO]` 前缀，`Errorf` 加 `[ERROR]` 前缀，底层走标准库 `log` 包。

## 七、API 速查表

### Simple

| 方法 | 说明 |
| --- | --- |
| `NewPubSimple(queueName, mqURL, opts...) (*MQSimple, error)` | 构造发布端 |
| `NewConsumeSimple(queueName, mqURL, opts...) (*MQSimple, error)` | 构造消费端 |
| `Publish(body []byte) (msgID string, err error)` | 发布到主队列 |
| `Consume(handler) error` | 阻塞消费主队列，失败走 retry |
| `PublishDelay(body []byte, ttl time.Duration) (string, error)` | 发布延迟消息 |
| `ConsumeDelay(handler) error` | 等价于 `Consume`（语义占位） |
| `PublishWithDlx(body []byte) (string, error)` | 声明 DLX 拓扑后发布 |
| `ConsumeFailToDlx(handler) error` | 失败直接进入 DLX |
| `ConsumeDlx(handler) error` | 消费死信队列 |
| `RetryMsg(msg amqp.Delivery, ttl time.Duration) error` | 手动把当前 delivery 投入 retry queue |

### Direct

| 方法 | 说明 |
| --- | --- |
| `NewPubDirect(exchange, routingKey, mqURL, opts...) (*MQDirect, error)` | 构造发布端 |
| `NewConsumeDirect(exchange, routingKey, mqURL, opts...) (*MQDirect, error)` | 构造消费端 |
| `Publish(body []byte) (string, error)` | 发布到 direct exchange |
| `Consume(handler) error` | 失败走 retry |
| `PublishDelay(body []byte, ttl time.Duration) (string, error)` | 延迟发布 |
| `ConsumeDelay(handler) error` | 等价于 `Consume` |
| `ConsumeFailToDlx(handler) error` | 失败直接 DLX |
| `ConsumeDlx(handler) error` | 消费死信队列 |
| `RetryMsg(msg amqp.Delivery, ttl time.Duration) error` | 手动投 retry |

### Fanout

| 方法 | 说明 |
| --- | --- |
| `NewPubFanout(exchange, mqURL, opts...) (*MQFanout, error)` | 构造发布端 |
| `NewConsumeFanout(exchange, mqURL, opts...) (*MQFanout, error)` | 构造消费端 |
| `Publish(body []byte) (string, error)` | 广播 |
| `Consume(handler) error` | 失败直接 `Reject(false)`，不重试 |
| `PublishDelay(body []byte, ttl time.Duration) (string, error)` | 延迟广播 |
| `ConsumeDelay(handler) error` | 等价于 `Consume` |
| `ConsumeFailToDlx(handler) error` | 失败直接 DLX |
| `ConsumeDlx(handler) error` | 消费死信队列 |

### Topic

| 方法 | 说明 |
| --- | --- |
| `NewPubTopic(exchange, routingKey, mqURL, opts...) (*MQTopic, error)` | 构造发布端 |
| `NewConsumeTopic(exchange, routingKey, mqURL, opts...) (*MQTopic, error)` | 构造消费端 |
| `Publish(body []byte) (string, error)` | 发布到 topic exchange |
| `Consume(handler) error` | 失败走 retry |
| `PublishDelay(body []byte, ttl time.Duration) (string, error)` | 延迟发布 |
| `ConsumeDelay(handler) error` | 等价于 `Consume` |
| `ConsumeFailToDlx(handler) error` | 失败直接 DLX |
| `ConsumeDlx(handler) error` | 消费死信队列 |
| `RetryMsg(msg amqp.Delivery, ttl time.Duration) error` | 手动投 retry |

> v1.1.0 起：
>
> - `Publish` / `PublishDelay` / `PublishWithDlx` 不再接受 `handler` 形参，发布失败通过 `WithPubFailNotify` 回调
> - 类型 `Mq*` 统一改为 `MQ*`（缩写大写）；公共 `RabbitMQ` 改名为 `MQ`，`RabbitMQInterface` 改名为 `MQInterface`
> - `RetryTTL` / `PublishDelay TTL` / `RetryMsg TTL` 类型由毫秒字符串改为 `time.Duration`
> - `FailedMsg.MsgId` → `FailedMsg.MessageID`、`MQOption.Routing` → `RoutingKey`、`MQOption.MqURL` → `MQURL`
> - 历史构造函数 `NewMQTopic` 已删除
> - `MQInterface` 收窄为各模式均真实支持的最小公共集合；新增 `Retrier` 子接口（simple/direct/topic 实现，fanout 不实现）
> - `MQOption` 不再嵌入 `MQ`，外部无法绕过 Options 直接改字段
> - `Destroy()` 现在会等待 in-flight Publish 完成；Destroy 之后再调用 Publish 返回 `ErrDestroyed`
> - `handler.Process` / `MsgHandler.Failed` / `WithPubFailNotify` 回调全部 `recover()` 保护，业务 panic 不会杀掉消费/发布 goroutine
> - 新增 5 个错误哨兵：`ErrDestroyed` / `ErrPublishNotAcknowledged` / `ErrConnectionNotInitialized` / `ErrHandlerRequired` / `ErrNotInitialized`，可用 `errors.Is` 判定
> - 新增 5 个 Option：`WithObserver` / `WithVhost` / `WithHeartbeat` / `WithPrefetchCount` / `WithTLSConfig`

## 八、完整示例

仓库内已经提供可直接运行的示例目录：

- `examples/simple/producer/main.go`
- `examples/simple/consumer/main.go`
- `examples/direct/producer/main.go`
- `examples/direct/consumer/main.go`
- `examples/fanout/producer/main.go`
- `examples/fanout/consumer/main.go`
- `examples/topic/producer/main.go`
- `examples/topic/consumer/main.go`

本地运行（需先启动 RabbitMQ）：

```bash
go run ./examples/direct/consumer
go run ./examples/direct/producer
```

### 1. Direct 模式发布示例

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/gtkit/rabbit"
)

func main() {
    mq, err := rabbit.NewPubDirect(
        "order.exchange",
        "order.created",
        "amqp://guest:guest@127.0.0.1:5672/",
        rabbit.WithContext(context.Background()),
        rabbit.WithConnectionName("order-producer"),
        rabbit.WithRetryTTL(3*time.Second),
        rabbit.WithMaxRetry(5),
        rabbit.WithPubFailNotify(func(msg rabbit.FailedMsg) {
            log.Printf("publish failed, msgID=%s body=%s", msg.MessageID, msg.Message)
        }),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer mq.Destroy()

    msgID, err := mq.Publish([]byte("create order #1001"))
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("publish ok: msgID=%s", msgID)
}
```

### 2. Direct 模式消费示例

```go
package main

import (
    "context"
    "errors"
    "log"
    "time"

    "github.com/gtkit/rabbit"
)

type handler struct{}

func (h *handler) Process(body []byte, msgID string) error {
    log.Printf("consume msgID=%s body=%s", msgID, string(body))

    if string(body) == "bad" {
        return errors.New("mock business error")
    }
    return nil
}

func (h *handler) Failed(msg rabbit.FailedMsg) {
    log.Printf("finally failed, msgID=%s body=%s", msg.MessageID, msg.Message)
}

func main() {
    mq, err := rabbit.NewConsumeDirect(
        "order.exchange",
        "order.created",
        "amqp://guest:guest@127.0.0.1:5672/",
        rabbit.WithContext(context.Background()),
        rabbit.WithConnectionName("order-consumer"),
        rabbit.WithQueueName("order.created.queue"),
        rabbit.WithMaxRetry(3),
        rabbit.WithRetryTTL(2*time.Second),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer mq.Destroy()

    if err := mq.Consume(&handler{}); err != nil {
        log.Fatal(err)
    }
}
```

### 3. Simple 延迟消息示例

```go
mq, err := rabbit.NewPubSimple(
    "sms.queue",
    "amqp://guest:guest@127.0.0.1:5672/",
)
if err != nil {
    return err
}
defer mq.Destroy()

if _, err := mq.PublishDelay([]byte("send after 10 seconds"), 10*time.Second); err != nil {
    return err
}
```

### 4. 接 Prometheus（Observer 钩子）

```go
import (
    "github.com/gtkit/rabbit"
    "github.com/prometheus/client_golang/prometheus"
)

type promObserver struct {
    publishTotal *prometheus.CounterVec
    publishHist  *prometheus.HistogramVec
    consumeTotal *prometheus.CounterVec
}

func (o *promObserver) OnPublish(e rabbit.PublishEvent) {
    status := "ok"
    if e.Err != nil {
        status = "fail"
    }
    o.publishTotal.WithLabelValues(e.Mode, e.Operation, status).Inc()
    o.publishHist.WithLabelValues(e.Mode, e.Operation).Observe(e.Duration.Seconds())
}

func (o *promObserver) OnConsume(e rabbit.ConsumeEvent) {
    status := "ok"
    if e.Err != nil {
        status = "fail"
    }
    o.consumeTotal.WithLabelValues(e.Mode, e.Operation, status).Inc()
}

func (o *promObserver) OnReconnect(e rabbit.ReconnectEvent) {
    log.Printf("mq reconnect: mode=%s op=%s attempt=%d err=%v", e.Mode, e.Operation, e.Attempt, e.Err)
}

// 注入：
mq, _ := rabbit.NewConsumeSimple(queue, mqURL, rabbit.WithObserver(observer))
```

### 5. 注入 `gtkit/logger`

```go
import (
    gtlogger "github.com/gtkit/logger"
    "github.com/gtkit/rabbit"
)

func init() {
    if !rabbit.SetLogger(gtlogger.Default()) {
        panic("inject gtkit logger failed")
    }
}
```

## 九、失败重试机制

适用于 `simple` / `direct` / `topic`。`fanout` 不重试（重投会让所有订阅者再次收到消息）。

流程：

1. 消费到消息
2. 调用 `handler.Process(body, msgID)`（recover 保护）
3. 返回 `nil` → `Ack`
4. 返回 `error` 或 panic → 读取 `x-retry` header
5. 未超过 `MaxRetry` → 进入 retry queue，TTL 到期后回到原始队列
6. 超过最大重试次数 → `Reject(false)`

补充：

- retry 次数记录在 `Headers["x-retry"]`
- 默认 `MaxRetry = 3`、`RetryTTL = 2s`

### 关于 `Ack` 失败

retry 消息发布成功但原消息 `Ack` 失败时：库记录错误日志、不退出消费循环。仍是 `at-least-once` 语义，业务需要基于 `msgID` 自行幂等。

## 十、死信机制

- `Consume`：失败先重试，超限后 `Reject(false)` 进入 DLX
- `ConsumeFailToDlx`：失败直接 `Reject(false)`，不重试
- `ConsumeDlx`：专门消费已经死信的消息

各模式的死信拓扑命名：

- `simple`：`dlx-<queue>` exchange + `dlq-<queue>` queue
- `direct` / `topic`：`<exchange>.dlx` exchange + `<base>.dlq` queue（`<base>` 取 queueName，未设置则取 `exchange.routingKey`）
- `fanout`：`<exchange>.dlx` exchange + `<base>.dlq` queue

## 十一、自动重连机制

### Consumer 重连

处理两类异常：channel 关闭、connection 关闭。当 RabbitMQ 重启或网络闪断时：

1. 重建 connection
2. 重建 consumer channel
3. 重新声明消费拓扑
4. 继续消费

### 退避策略

`1s → 2s → 4s → 8s → 16s → 30s → 30s ...`，重新建立消费成功后退避计数清零。

### Publisher 重连

发布端复用 confirm channel；channel/connection 不可用时自动重建，新 channel 上需要的 exchange/queue 重新声明（声明缓存随之重置）。

## 十二、注意事项

1. **`Destroy()` 必须调用**：`defer mq.Destroy()`
2. **`Consume` 系列阻塞**：放在 goroutine 中或作为进程主循环
3. **`fanout` 不做自动 retry**：是有意设计
4. **业务自己做幂等**：基于 `msgID` 或业务唯一键
5. **`handler.Process` 不会让消费 goroutine 挂掉**：panic 会被 `recover` 并记录，按 retry/reject 规则处理

## 十三、测试

### 1. 单元测试（不依赖 RabbitMQ）

```bash
go test ./rabbit/...
```

### 2. 全量（含 race）

```bash
GOWORK=off go test -race -count=1 ./...
```

### 3. 集成测试（需要 RabbitMQ）

```bash
MQ_INTEGRATION=1 go test ./test -count=1
```

自定义连接：

```bash
MQ_INTEGRATION=1 MQ_URL=amqp://guest:guest@127.0.0.1:5672/ go test ./test -count=1
```

未设置 `MQ_INTEGRATION=1` 时，`./test` 下的用例会跳过。

## 十四、永久错误 fast-fail（v1.2.0+）

v1.2.0 引入了"协议层永久错误"快速失败机制。基于 `amqp091-go v1.11+` 提供的
`*amqp.Error.Temporary()` 协议错误码分类，库内部能识别 broker 端"重试无效、需要外部干预"的错误：

- 凭证错误（access-refused / no access to vhost）
- 队列 / 交换机不存在或类型冲突（not-found / precondition-failed）
- 权限不足

碰到这类错误时，**消费循环立刻退出**而不是无脑指数退避，节省时间也便于上游告警。

### 用法

```go
err := mq.Consume(handler)
if errors.Is(err, rabbit.ErrPermanent) {
    // 需要外部干预（运维 / 配置），retry 无效
    log.Fatalf("permanent broker error: %v", err)
}
```

`ReconnectEvent` 也带 `Permanent bool` 字段，Observer 可以在监控面板上区分"普通重连"和"已退出的硬错误"。

## 十五、版本

当前 `v1.2.0`。

- v1.0 → v1.1.0：破坏性 API 重构 + Observer 钩子 + Retrier + 5 个错误哨兵 + 4 个新 Option
- v1.1.0 → v1.2.0：`amqp091-go v1.10 → v1.11`，新增 `ErrPermanent` fast-fail 机制（向后兼容）
