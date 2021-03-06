/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

//引用资源
public abstract class ReferenceResource {
    //引用计数
    protected final AtomicLong refCount = new AtomicLong(1);
    //引用的对象是否可用
    protected volatile boolean available = true;
    //清除对象
    protected volatile boolean cleanupOver = false;
    //第一次关闭的时间戳
    private volatile long firstShutdownTimestamp = 0;

    //增加引用计数
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    //关闭逻辑
    public void shutdown(final long intervalForcibly) {
        //第一次 设置成不可用
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            //释放资源  这里就是减少引用计数 并没有真正释放元素 除非是负数
            this.release();
        } else if (this.getRefCount() > 0) {
            //时间间隔大于 强制关闭的时间间隔  就是允许关闭
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                //设置成一个负数 就会释放元素
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;

        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
