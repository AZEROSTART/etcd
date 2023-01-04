// Copyright 2016 The etcd Authors
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

package schedule

import (
	"context"
	"sync"
)

type Job func(context.Context)

// Scheduler can schedule jobs.
type Scheduler interface {
	// Schedule asks the scheduler to schedule a job defined by the given func.
	// Schedule to a stopped scheduler might panic.
	Schedule(j Job)

	// Pending returns number of pending jobs
	Pending() int

	// Scheduled returns the number of scheduled jobs (excluding pending jobs)
	Scheduled() int

	// Finished returns the number of finished jobs
	Finished() int

	// WaitFinish waits until at least n job are finished and all pending jobs are finished.
	WaitFinish(n int)

	// Stop stops the scheduler.
	Stop()
}

type fifo struct {
	mu sync.Mutex

	resume    chan struct{} // 恢复
	scheduled int
	finished  int
	pendings  []Job // 一个job就是一个韩素

	ctx    context.Context
	cancel context.CancelFunc

	finishCond *sync.Cond
	donec      chan struct{}
}

// NewFIFOScheduler returns a Scheduler that schedules jobs in FIFO
// order sequentially
func NewFIFOScheduler() Scheduler {
	f := &fifo{
		resume: make(chan struct{}, 1),
		donec:  make(chan struct{}, 1),
	}
	f.finishCond = sync.NewCond(&f.mu)
	f.ctx, f.cancel = context.WithCancel(context.Background())
	go f.run()
	return f
}

// Schedule schedules a job that will be ran in FIFO order sequentially. 有意思，抽象了一个队列出来
func (f *fifo) Schedule(j Job) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// 将所有错误都收进来。比如在stop之后调用了服务，此时就应该panic。
	if f.cancel == nil {
		panic("schedule: schedule to stopped scheduler")
	}

	// 提供一个唤醒机制，这样就不会一直占用cpu了！不错
	if len(f.pendings) == 0 {
		select {
		case f.resume <- struct{}{}:
		default:
		}
	}
	f.pendings = append(f.pendings, j)
}

func (f *fifo) Pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pendings)
}

// 返回已经调度成功了的数量；这个是形容词。
func (f *fifo) Scheduled() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.scheduled
}

func (f *fifo) Finished() int {
	f.finishCond.L.Lock()
	defer f.finishCond.L.Unlock()
	return f.finished
}

//
func (f *fifo) WaitFinish(n int) {
	f.finishCond.L.Lock()
	for f.finished < n || len(f.pendings) != 0 {
		f.finishCond.Wait()
	}
	f.finishCond.L.Unlock()
}

// Stop stops the scheduler and cancels all pending jobs.
func (f *fifo) Stop() {
	f.mu.Lock()
	f.cancel()
	f.cancel = nil
	f.mu.Unlock()
	// 牛逼，这个也是细节！！！只有等待run函数结束之后，你才会返回。
	<-f.donec // 这是接收方！！！！！
}

func (f *fifo) run() {
	// TODO: recover from job panic?
	defer func() {
		close(f.donec) // 直接关闭chan，然后自动通知stop，ok了。
		close(f.resume)
	}()

	for {
		var todo Job
		// 临界区
		f.mu.Lock()
		if len(f.pendings) != 0 {
			f.scheduled++ // 需要schedule的任务
			todo = f.pendings[0]
		}
		f.mu.Unlock()

		if todo == nil {
			select {
			case <-f.resume:
			case <-f.ctx.Done():
				f.mu.Lock()
				pendings := f.pendings
				f.pendings = nil
				f.mu.Unlock()
				// clean up pending jobs				优雅关闭的前提就是catch住退出的信号，然后做完了再退出。
				for _, todo := range pendings {
					todo(f.ctx)
				}
				return
			}
		} else {
			todo(f.ctx)
			f.finishCond.L.Lock()
			f.finished++
			f.pendings = f.pendings[1:]
			f.finishCond.Broadcast() // 通知lock的chan；locked的 chan通过一个for循环一直wait，完成一次通知一次。
			f.finishCond.L.Unlock()
		}
	}
}
