package rabbit

import (
	"fmt"
	"log"
	"sync/atomic"
)

var _ Logger = (*Log)(nil)

// Logger 定义了库内部使用的最小日志接口。
// 调用方实现该接口后通过 SetLogger 或 WithLogger 注入。
type Logger interface {
	Info(args ...any)
	Infof(template string, args ...any)
	Errorf(template string, args ...any)
}

// Log 是基于标准库 log 的默认实现。
// Info/Infof 加 [INFO] 前缀，Errorf 加 [ERROR] 前缀。
type Log struct{}

// loggerHolder 仅用来作为 atomic.Value 的 type-safe 包装，避免每次 Load 都做类型断言。
type loggerHolder struct {
	logger Logger
}

// formattedLogger 是大多数项目级 logger 都会实现的最小子集（Infof + Errorf）。
type formattedLogger interface {
	Infof(template string, args ...any)
	Errorf(template string, args ...any)
}

// infoLogger 是可选的额外能力：未实现时 adaptedLogger.Info 会退化成 Infof("%s", ...)。
type infoLogger interface {
	Info(args ...any)
}

// adaptedLogger 把仅满足 formattedLogger 的对象适配成 Logger。
// 如果对象同时实现 infoLogger，则 Info 走原生实现；否则降级到 Infof。
type adaptedLogger struct {
	formatted formattedLogger
	info      infoLogger
}

// loggerValue 是全局 logger 的 atomic 容器（per-instance logger 通过 WithLogger 覆盖）。
var loggerValue atomic.Value

// Info 输出 [INFO] 前缀的日志，参数按 log.Println 风格拼接。
func (l *Log) Info(args ...any) {
	log.Println(append([]any{"[INFO]"}, args...)...)
}

// Infof 输出 [INFO] 前缀的格式化日志。
func (l *Log) Infof(template string, args ...any) {
	log.Printf("[INFO] "+template, args...)
}

// Errorf 输出 [ERROR] 前缀的格式化日志（默认 Log 实现不区分 stderr，只是前缀不同）。
func (l *Log) Errorf(template string, args ...any) {
	log.Printf("[ERROR] "+template, args...)
}

// NewLogger 返回默认实现。
func NewLogger() Logger {
	return &Log{}
}

// SetLogger 注入全局 logger。
// 入参可以是 Logger，也可以是仅实现 Infof/Errorf 的对象（自动适配）。
// 不匹配时返回 false 且不更改全局 logger。
// 实例级别可使用 WithLogger Option 覆盖。
func SetLogger(l any) bool {
	logger, ok := adaptLogger(l)
	if !ok {
		return false
	}

	loggerValue.Store(loggerHolder{logger: logger})
	return true
}

// adaptLogger 将任意输入转换为 Logger，失败返回 ok=false。
// 适配优先级：实现 Logger > 实现 formattedLogger（自动包成 adaptedLogger）> 不支持。
func adaptLogger(l any) (Logger, bool) {
	if l == nil {
		return nil, false
	}

	if logger, ok := l.(Logger); ok {
		return logger, true
	}

	formatted, ok := l.(formattedLogger)
	if !ok {
		return nil, false
	}

	var info infoLogger
	if typed, isInfo := l.(infoLogger); isInfo {
		info = typed
	}

	return adaptedLogger{
		formatted: formatted,
		info:      info,
	}, true
}

// currentLogger 返回当前全局 logger，nil 时懒初始化为默认实现。
func currentLogger() Logger {
	holder, _ := loggerValue.Load().(loggerHolder)
	logger := holder.logger
	if logger == nil {
		logger = NewLogger()
		loggerValue.Store(loggerHolder{logger: logger})
	}

	return logger
}

// Info 走原生 Info 或降级到 Infof("%s", ...)。
func (l adaptedLogger) Info(args ...any) {
	if l.info != nil {
		l.info.Info(args...)
		return
	}

	l.formatted.Infof("%s", fmt.Sprint(args...))
}

// Infof 透传到被适配对象的 Infof。
func (l adaptedLogger) Infof(template string, args ...any) {
	l.formatted.Infof(template, args...)
}

// Errorf 透传到被适配对象的 Errorf。
func (l adaptedLogger) Errorf(template string, args ...any) {
	l.formatted.Errorf(template, args...)
}
