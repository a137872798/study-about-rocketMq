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

/**
 * $Id: CreateTopicRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

//创建 topic 对象的 请求头
public class CreateTopicRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String topic;
    @CFNotNull
    private String defaultTopic;
    //这个 topic 下 可读队列 和 可写队列数
    @CFNotNull
    private Integer readQueueNums;
    @CFNotNull
    private Integer writeQueueNums;
    //权限
    @CFNotNull
    private Integer perm;
    //拦截类型  单个还是多个
    @CFNotNull
    private String topicFilterType;
    //topic的 特殊标识
    private Integer topicSysFlag;
    @CFNotNull
    private Boolean order = false;

    @Override
    public void checkFields() throws RemotingCommandException {
        try {
            //校验拦截类型
            TopicFilterType.valueOf(this.topicFilterType);
        } catch (Exception e) {
            throw new RemotingCommandException("topicFilterType = [" + topicFilterType + "] value invalid", e);
        }
    }

    public TopicFilterType getTopicFilterTypeEnum() {
        return TopicFilterType.valueOf(this.topicFilterType);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }

    public Integer getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(Integer readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public Integer getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(Integer writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public Integer getPerm() {
        return perm;
    }

    public void setPerm(Integer perm) {
        this.perm = perm;
    }

    public String getTopicFilterType() {
        return topicFilterType;
    }

    public void setTopicFilterType(String topicFilterType) {
        this.topicFilterType = topicFilterType;
    }

    public Integer getTopicSysFlag() {
        return topicSysFlag;
    }

    public void setTopicSysFlag(Integer topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }

    public Boolean getOrder() {
        return order;
    }

    public void setOrder(Boolean order) {
        this.order = order;
    }
}