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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.util.data.collect;

import java.util.Collection;

//数据 收集器
public interface DataCollector {

    void resetData();

    Collection<Object> getAllData();

    Collection<Object> getAllDataWithoutDuplicate();

    void addData(Object data);

    long getDataSizeWithoutDuplicate();

    long getDataSize();

    boolean isRepeatedData(Object data);

    int getRepeatedTimeForData(Object data);

    void removeData(Object data);

    void lockIncrement();

    void unlockIncrement();
}
