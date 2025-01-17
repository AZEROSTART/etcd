// Copyright 2017 The etcd Authors
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

package grpcproxy

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver/api/etcdhttp"
	"go.uber.org/zap"
)

// HandleHealth registers health handler on '/health'.
// 在start grpc的proxy会调用并注册
// health的检查，是通过去获取key"a"的值来确认是否ok
func HandleHealth(lg *zap.Logger, mux *http.ServeMux, c *clientv3.Client) {
	if lg == nil {
		lg = zap.NewNop()
	}
	mux.Handle(etcdhttp.PathHealth, etcdhttp.NewHealthHandler(lg, func(excludedAlarms etcdhttp.AlarmSet, serializable bool) etcdhttp.Health { return checkHealth(c) }))
}

// HandleProxyHealth registers health handler on '/proxy/health'.
func HandleProxyHealth(lg *zap.Logger, mux *http.ServeMux, c *clientv3.Client) {
	if lg == nil {
		lg = zap.NewNop()
	}
	mux.Handle(etcdhttp.PathProxyHealth, etcdhttp.NewHealthHandler(lg, func(excludedAlarms etcdhttp.AlarmSet, serializable bool) etcdhttp.Health { return checkProxyHealth(c) }))
}

func checkHealth(c *clientv3.Client) etcdhttp.Health {
	h := etcdhttp.Health{Health: "false"}
	ctx, cancel := context.WithTimeout(c.Ctx(), time.Second)
	_, err := c.Get(ctx, "a")			// health的检测的是获取key为a。如果ok说明正常。
	cancel()
	// 客户端的错误，并不代表server端的问题，所以是true的
	if err == nil || err == rpctypes.ErrPermissionDenied {
		h.Health = "true"
	} else {
		h.Reason = fmt.Sprintf("GET ERROR:%s", err)
	}
	return h
}

// 代理健康?
func checkProxyHealth(c *clientv3.Client) etcdhttp.Health {
	if c == nil {
		return etcdhttp.Health{Health: "false", Reason: "no connection to proxy"}
	}
	h := checkHealth(c)
	if h.Health != "true" {
		return h
	}
	ctx, cancel := context.WithTimeout(c.Ctx(), time.Second*3)			// 超时是3秒
	ch := c.Watch(ctx, "a", clientv3.WithCreatedNotify())
	// 可以用这样的方式做一个超时
	select {
	case <-ch:
	case <-ctx.Done():
		h.Health = "false"
		h.Reason = "WATCH TIMEOUT"
	}
	// 关闭所有的chan（contx拥有的）
	cancel()
	return h
}
