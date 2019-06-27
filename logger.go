// Copyright 2019 shimingyah. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// ee the License for the specific language governing permissions and
// limitations under the License.

package wisckey

import (
	"github.com/sirupsen/logrus"
)

// Logger is implemented by any logging system that is used for standard logs.
type Logger interface {
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})

	Error(v ...interface{})
	Errorf(format string, v ...interface{})

	Info(v ...interface{})
	Infof(format string, v ...interface{})

	Warning(v ...interface{})
	Warningf(format string, v ...interface{})

	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
}

type defaultLog struct{}

var defaultLogger = &defaultLog{}

func (rl *defaultLog) Debug(v ...interface{})                   { logrus.Debug(v...) }
func (rl *defaultLog) Debugf(format string, v ...interface{})   { logrus.Debugf(format, v...) }
func (rl *defaultLog) Error(v ...interface{})                   { logrus.Error(v...) }
func (rl *defaultLog) Errorf(format string, v ...interface{})   { logrus.Errorf(format, v...) }
func (rl *defaultLog) Info(v ...interface{})                    { logrus.Info(v...) }
func (rl *defaultLog) Infof(format string, v ...interface{})    { logrus.Infof(format, v...) }
func (rl *defaultLog) Warning(v ...interface{})                 { logrus.Warning(v...) }
func (rl *defaultLog) Warningf(format string, v ...interface{}) { logrus.Warningf(format, v...) }
func (rl *defaultLog) Fatal(v ...interface{})                   { logrus.Fatal(v...) }
func (rl *defaultLog) Fatalf(format string, v ...interface{})   { logrus.Fatalf(format, v...) }
