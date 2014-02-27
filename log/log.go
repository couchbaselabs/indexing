//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not
//  use this file except in compliance with the License. You may obtain a copy
//  of the License at,
//          http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//  License for the specific language governing permissions and limitations
//  under the License.

// Proxy for golang's standard log package. Adds additional functions to do
// debug/warn/info logging. Right now only a global level of logging is
// supported.
package api

import (
	"io"
	"log"
)

const (
	levelDebug byte = iota
	levelWarn  byte = iota
	levelInfo  byte = iota
)

// Loglevel for seconday index.
var loglevel byte

func SetLoglevel(level string) {
	switch level {
	case "debug":
		loglevel = levelDebug
	case "warn":
		loglevel = levelWarn
	case "info":
		loglevel = levelInfo
	}
}

// Debug logs with log.Print() if log-level is greater than or equal to
// levelDebug
func Debug(v ...interface{}) {
	if loglevel >= levelDebug {
		log.Print(v...)
	}
}

// Debugf logs with log.Printf() if log-level is greater than or equal to
// levelDebug
func Debugf(format string, v ...interface{}) {
	if loglevel >= levelDebug {
		log.Printf(format, v...)
	}
}

// Debugln logs with log.Println() if log-level is greater than or equal to
// levelDebug
func Debugln(v ...interface{}) {
	if loglevel >= levelDebug {
		log.Println(v...)
	}
}

// Warn logs with log.Print() if log-level is greater than or equal to
// levelWarn
func Warn(v ...interface{}) {
	if loglevel >= levelWarn {
		log.Print(v...)
	}
}

// Warnf logs with log.Printf() if log-level is greater than or equal to
// levelWarn
func Warnf(format string, v ...interface{}) {
	if loglevel >= levelWarn {
		log.Printf(format, v...)
	}
}

// Warnln logs with log.Println() if log-level is greater than or equal to
// levelWarn
func Warnln(v ...interface{}) {
	if loglevel >= levelWarn {
		log.Println(v...)
	}
}

// Info logs with log.Print() if log-level is greater than or equal to
// levelInfo
func Info(v ...interface{}) {
	if loglevel >= levelInfo {
		log.Print(v...)
	}
}

// Infof logs with log.Printf() if log-level is greater than or equal to
// levelInfo
func Infof(format string, v ...interface{}) {
	if loglevel >= levelInfo {
		log.Printf(format, v...)
	}
}

// Infoln logs with log.Println() if log-level is greater than or equal to
// levelInfo
func Infoln(v ...interface{}) {
	if loglevel >= levelInfo {
		log.Println(v...)
	}
}

// Print logs with log.Print() if log-level is greater than or equal to
// levelInfo
func Print(v ...interface{}) {
	if loglevel >= levelInfo {
		log.Print(v...)
	}
}

// Printf logs with log.Printf() if log-level is greater than or equal to
// levelInfo
func Printf(format string, v ...interface{}) {
	if loglevel >= levelInfo {
		log.Printf(format, v...)
	}
}

// Println logs with log.Println() if log-level is greater than or equal to
// levelInfo
func Println(v ...interface{}) {
	if loglevel >= levelInfo {
		log.Println(v...)
	}
}

// Fatal is proxy for log.Fatal()
func Fatal(v ...interface{}) {
	log.Fatal(v...)
}

// Fatalf is proxy for log.Fatalf()
func Fatalf(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}

// Fatalln is proxy for log.Fatalln()
func Fatalln(v ...interface{}) {
	log.Fatalln(v...)
}

// Panic is proxy for log.Panic()
func Panic(v ...interface{}) {
	log.Panic(v...)
}

// Panicf is proxy for log.Panicf()
func Panicf(format string, v ...interface{}) {
	log.Panicf(format, v...)
}

// Panicln is proxy for log.Panicln()
func Panicln(v ...interface{}) {
	log.Panicln(v...)
}

// Flags is proxy for log.Flags()
func Flags() int {
	return log.Flags()
}

// Prefix is proxy for log.Prefix()
func Prefix() string {
	return log.Prefix()
}

// SetFlags is proxy for log.SetFlags()
func SetFlags(flag int) {
	log.SetFlags(flag)
}

// SetOutput is proxy for log.SetOutput()
func SetOutput(w io.Writer) {
	log.SetOutput(w)
}

// Flags is proxy for log.Flags()
func SetPrefix(prefix string) {
	log.SetPrefix(prefix)
}
