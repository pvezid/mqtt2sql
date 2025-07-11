/*
  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.

  Copyright © 2025 Georges Ménie.
*/

package main

import (
	"flag"
	"fmt"
	"log/slog"
	"menie.org/mqtt2sql/handlers"
	"os"
)

var (
	brokerURL string
	subtopic  string
	infile    string
	debugmode bool
)

func main() {

	setFlags()
	setLogger()

	if subtopic == "" && infile == "" {
		slog.Error("Topic not specified, use '-s topic'")
		return
	}

	if infile != "" {
		ch1 := handlers.FileHandler(infile)
		ch2 := handlers.JSONHandler(ch1)
		handlers.SqlBatchHandler(ch2)
		os.Exit(0)
	}

	if ch1 := handlers.MQTTHandler(brokerURL, subtopic); ch1 != nil {
		ch2 := handlers.JSONHandler(ch1)
		handlers.SqlHandler(ch2)
	}
}

func init() {
	flag.StringVar(&brokerURL, "h", "tcp://mqtt:1883", "MQTT broker to use")
	flag.StringVar(&subtopic, "s", "", "topic to be subscribed")
	flag.StringVar(&infile, "r", "", "input file, replacing mqtt input")
	flag.BoolVar(&debugmode, "debug", false, "set loglevel to DEBUG")
}

func setFlags() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [options]\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "Options:\n")
		flag.PrintDefaults()
	}
	flag.Parse()
}

func setLogger() {
	logLevel := &slog.LevelVar{} // INFO par défaut
	if debugmode {
		logLevel.Set(slog.LevelDebug)
	}
	opts := &slog.HandlerOptions{
		Level: logLevel,
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, opts))
	slog.SetDefault(logger)
}
