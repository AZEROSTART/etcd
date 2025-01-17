// Copyright 2015 The etcd Authors
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
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import "github.com/prometheus/client_golang/prometheus"

var (
	walFsyncSec = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "etcd",
		Subsystem: "disk",
		Name:      "wal_fsync_duration_seconds",
		Help:      "The latency distributions of fsync called by WAL.",

		// lowest bucket start of upper bound 0.001 sec (1 ms) with factor 2
		// highest bucket start of 0.001 sec * 2^13 == 8.192 sec
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14), //histogram可以定义最低喝最高值。
	})

	walWriteBytes = prometheus.NewGauge(prometheus.GaugeOpts{ // geige
		Namespace: "etcd",
		Subsystem: "disk",
		Name:      "wal_write_bytes_total",
		Help:      "Total number of bytes written in WAL.",
	})
)

// 导入这个包就会初始化，这个地方用的不错，学习了。
func init() {
	prometheus.MustRegister(walFsyncSec) // 提供url？
	prometheus.MustRegister(walWriteBytes)
}
