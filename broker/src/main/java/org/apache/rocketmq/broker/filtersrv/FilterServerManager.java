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

package org.apache.rocketmq.broker.filtersrv;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerStartup;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;

//拦截服务
public class FilterServerManager {

    public static final long FILTER_SERVER_MAX_IDLE_TIME_MILLS = 30000;
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    //针对指定channel 进行拦截
    private final ConcurrentMap<Channel, FilterServerInfo> filterServerTable =
        new ConcurrentHashMap<Channel, FilterServerInfo>(16);
    private final BrokerController brokerController;

    private ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("FilterServerManagerScheduledThread"));

    public FilterServerManager(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    //定时创建拦截对象
                    FilterServerManager.this.createFilterServer();
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        }, 1000 * 5, 1000 * 30, TimeUnit.MILLISECONDS);
    }

    public void createFilterServer() {
        //拦截对象不足就创建对象
        int more =
            this.brokerController.getBrokerConfig().getFilterServerNums() - this.filterServerTable.size();
        //创建 shell指令
        String cmd = this.buildStartCommand();
        for (int i = 0; i < more; i++) {
            //执行shell指令
            FilterServerUtil.callShell(cmd, log);
        }
    }

    //创建命令
    private String buildStartCommand() {
        String config = "";
        if (BrokerStartup.configFile != null) {
            config = String.format("-c %s", BrokerStartup.configFile);
        }

        if (this.brokerController.getBrokerConfig().getNamesrvAddr() != null) {
            config += String.format(" -n %s", this.brokerController.getBrokerConfig().getNamesrvAddr());
        }

        if (RemotingUtil.isWindowsPlatform()) {
            return String.format("start /b %s\\bin\\mqfiltersrv.exe %s",
                this.brokerController.getBrokerConfig().getRocketmqHome(),
                config);
        } else {
            return String.format("sh %s/bin/startfsrv.sh %s",
                this.brokerController.getBrokerConfig().getRocketmqHome(),
                config);
        }
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    //创建拦截服务信息
    public void registerFilterServer(final Channel channel, final String filterServerAddr) {
        FilterServerInfo filterServerInfo = this.filterServerTable.get(channel);
        if (filterServerInfo != null) {
            filterServerInfo.setLastUpdateTimestamp(System.currentTimeMillis());
        } else {
            filterServerInfo = new FilterServerInfo();
            filterServerInfo.setFilterServerAddr(filterServerAddr);
            filterServerInfo.setLastUpdateTimestamp(System.currentTimeMillis());
            this.filterServerTable.put(channel, filterServerInfo);
            log.info("Receive a New Filter Server<{}>", filterServerAddr);
        }
    }

    //删除拦截服务中的 过期channel
    public void scanNotActiveChannel() {

        Iterator<Entry<Channel, FilterServerInfo>> it = this.filterServerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, FilterServerInfo> next = it.next();
            long timestamp = next.getValue().getLastUpdateTimestamp();
            Channel channel = next.getKey();
            if ((System.currentTimeMillis() - timestamp) > FILTER_SERVER_MAX_IDLE_TIME_MILLS) {
                log.info("The Filter Server<{}> expired, remove it", next.getKey());
                it.remove();
                RemotingUtil.closeChannel(channel);
            }
        }
    }

    //移除指定channel
    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        FilterServerInfo old = this.filterServerTable.remove(channel);
        if (old != null) {
            log.warn("The Filter Server<{}> connection<{}> closed, remove it", old.getFilterServerAddr(),
                remoteAddr);
        }
    }

    public List<String> buildNewFilterServerList() {
        List<String> addr = new ArrayList<>();
        Iterator<Entry<Channel, FilterServerInfo>> it = this.filterServerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, FilterServerInfo> next = it.next();
            addr.add(next.getValue().getFilterServerAddr());
        }
        return addr;
    }

    static class FilterServerInfo {
        private String filterServerAddr;
        private long lastUpdateTimestamp;

        public String getFilterServerAddr() {
            return filterServerAddr;
        }

        public void setFilterServerAddr(String filterServerAddr) {
            this.filterServerAddr = filterServerAddr;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }
    }
}
