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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

//获取mq 的 策略对象
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    //延迟容忍  维护broker 发送消息的延迟
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    //是否容许延迟
    private boolean sendLatencyFaultEnable = false;

    //延迟段  这里记录的是一个 反应时间 就是 从发送消息开始 到获取响应 超过指定的时间段 就代表这个broker 不可用了
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    //不可用时长
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    //根据 topic 和 broker 选择合适的mq队列  也就是选择 要发到哪个broker 才合适 上次的broker 会影响本次选择结果
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        //允许故障延迟  就是处理掉无效的 broker 以及检测 broker 还能不能用
        //如果不允许 直接 进入获取到 发送到某个broker 的 mq 对象 那么 可能broker 会故障 就会无法发送消息
        if (this.sendLatencyFaultEnable) {
            try {
                //这里 会先检查 broker 是否可用
                //这个值是每个线程 生成的随机数
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                //获取 可写的 消息队列数量 这里 对应的是 从路由信息中获取到 queueData 并且 在具备写权限 和对应的 写mq数量 以及
                //对应的broker master可用的情况下 设置的 每个 mq对象 会有mqId 一样的情况 但是broker 会不一样 因为发送往的broker不一样
                //并且这个mq 对象还是针对指定的 topic消息 进行发送
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    //获取一个随机的 mq 对象 确定mq 对象的同时 也确定了要发送的broker 对象
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    //检验这个mq对象对应的 这个broker 是否可用 第一次 都是可用的然后 将 延迟信息保存到容器中 下次再判断是否可用
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        //是可用的情况下 返回这个mq 如果不可用 获取下一个 mq对象
                        //如果带上了 上次的 broker 信息 一旦broker 对不上就 不能发送 为什么这么做???
                        //保证将 消息发送到一个 broker 上吗?
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }

                //broker 都没对上 从 管理broker的 这个对象中选出一个相对合适的broker 这里 就不管上次的 broker 是哪个了
                //并且 极端情况可能所有broker 都不可用 之后发送消息就会出问题了 但是有重试机制
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                //获得 针对于 这个broker 这个topic 可以有多少个mq 进行写入数据
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    //随机获取一个mq
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        //这里把 mq 的数据修改了  这个mq 可能原来不是这个broker的
                        //即使某个broker下的mq 可能全被修改 一旦从路由信息中获取到 broker 的写队列数量还是会在这里修改mq 信息
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    //如果获取到的queuedata没有可写的 就将 该broker 信息移除 因为这个broker名字下没有对应的mq
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            //调用默认的获取随机 mq的方法 选择mq 的同时 代表选择了 broker
            return tpInfo.selectOneMessageQueue();
        }

        //默认情况不使用故障延时机制  这里会返回跟上次不同的 broker下的mq 对象
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }


    //2个 容器一个 一个问答的 间隔时间一个 对应延迟时间 相互对应 使得在指定时间内不重复使用某个broker 减轻broker 负荷



    //每次收到 应答的时候都会更新这个值 当 发生异常时 后面的参数是 true duration 就是30000
    //30000 超过了 最大的 15000 也即是在 60000中都不会调用 在这期间 broker 应该会自动修复
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            //设置 延迟段 使得在指定的 时间内 不使用 该 broker 也算是一种限流方式
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
