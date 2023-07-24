/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"errors"
	"fmt"
	"path"
	"path/filepath"

	"github.com/influxdata/influxdb/toml"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

const (
	// DefaultSubPath is default subpath for storing logs
	DefaultSubPath = "logs"
	// DefaultLevel is the level of logs will be emitted
	DefaultLevel = zap.InfoLevel

	// DefaultMaxSize is the max size of a log file
	DefaultMaxSize = 64 * 1024 * 1024 // 64MB

	// DefaultMaxNum is the max number of log files
	DefaultMaxNum = 16

	// DefaultMaxAge is the max duration a log file can keep
	DefaultMaxAge = 7 // 7days

	// DefaultCompressEnabled is whether the log files are compressed
	DefaultCompressEnabled = true
)

// global readonly logger
var globalLogger *Logger

func GetStoreLogger() *Logger {
	return globalLogger
}

type Logger struct {
	app             App
	Format          string        `toml:"format"`
	Level           zapcore.Level `toml:"level"`
	MaxSize         toml.Size     `toml:"max-size"`
	MaxNum          int           `toml:"max-num"`
	MaxAge          int           `toml:"max-age"`
	CompressEnabled bool          `toml:"compress-enabled"`
	Path            string        `toml:"path"`
}

// NewLogger returns a new instance of Config with defaults.
func NewLogger(app App) Logger {
	logger := Logger{
		app:             app,
		Format:          "auto",
		Level:           DefaultLevel,
		MaxSize:         toml.Size(DefaultMaxSize),
		MaxNum:          DefaultMaxNum,
		MaxAge:          DefaultMaxAge,
		CompressEnabled: DefaultCompressEnabled,
		Path:            filepath.Join(openGeminiDir(), DefaultSubPath),
	}
	globalLogger = &logger
	return logger
}

// Validate validates that the configuration is acceptable.
func (c Logger) Validate() error {
	if c.MaxSize <= 0 {
		return errors.New("logger max-size must be positive")
	}

	if c.MaxNum <= 0 {
		return errors.New("logger max-num must be positive")
	}

	if c.MaxAge <= 0 {
		return errors.New("logger max-age must be positive")
	}

	if c.Path == "" {
		return errors.New("logger path must not be empty")
	}

	return nil
}

func (c *Logger) SetApp(app App) {
	c.app = app
}

func (c *Logger) GetFileName() string {
	return c.filename(string(c.app))
}

func (c *Logger) filename(name string) string {
	return path.Clean(fmt.Sprintf("%s/%s.log", c.Path, name))
}

func (c *Logger) Build(name string) *lumberjack.Logger {
	return &lumberjack.Logger{
		Filename:   c.filename(name),
		MaxSize:    int(c.MaxSize),
		MaxBackups: c.MaxNum,
		MaxAge:     c.MaxAge,
		Compress:   c.CompressEnabled,
	}
}
