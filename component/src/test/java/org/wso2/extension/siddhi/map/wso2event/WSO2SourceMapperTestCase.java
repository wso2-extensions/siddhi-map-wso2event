/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.map.wso2event;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class WSO2SourceMapperTestCase {
    private static final Logger log = Logger.getLogger(WSO2SourceMapperTestCase.class);
    private AtomicInteger count = new AtomicInteger();
    private org.wso2.carbon.databridge.commons.Event wso2event;
    private org.wso2.carbon.databridge.commons.Event wso2event1;
    private org.wso2.carbon.databridge.commons.Event wso2event2;

    @Before
    public void init() {
        count.set(0);
        Object mata[] = {1496814501L};
        Object correlation[] = {"IBM", 500.5f, 500};
        Object payload[] = {"WSO2", 100.5f, 50};
        Map<String, String> arbitraryDataMap = new HashMap<>();
        arbitraryDataMap.put("key1", "value1");
        arbitraryDataMap.put("key2", "value2");
        wso2event = new org.wso2.carbon.databridge.commons.Event("org" +
                ".fooStream:1.0.0", 1496814773L, mata, correlation, payload, arbitraryDataMap);

        Object mata1[] = {1496815144L};
        Object correlation1[] = {"IBM", 500.5f, 500};
        Object payload1[] = {"WSO22", 101.5f, 51};
        Map<String, String> arbitraryDataMap1 = new HashMap<>();
        arbitraryDataMap1.put("key11", "value11");
        arbitraryDataMap1.put("key22", "value22");
        wso2event1 = new org.wso2.carbon.databridge.commons.Event("org" +
                ".fooStream:1.0.0", 1496815276L, mata1, correlation1, payload1, arbitraryDataMap1);

        Object mata2[] = {1496815144L};
        Object correlation2[] = {"IBM", 500.5f, 500};
        Object payload2[] = {"WSO222", 102.5f, 52};
        Map<String, String> arbitraryDataMap2 = new HashMap<>();
        arbitraryDataMap2.put("key111", "value111");
        arbitraryDataMap2.put("key222", "value222");
        wso2event2 = new org.wso2.carbon.databridge.commons.Event("org" +
                ".fooStream:1.0.0", 1496815276L, mata2, correlation2, payload2, arbitraryDataMap2);
    }

    /**
     * Expected input format:
     * Wso2event = {
     *        streamId: wso2event.fooStream:1.0.0,
     *        timeStamp: 431434134134,
     *        metaData: [meta_object1, meta_object2],
     *        correlationData: [correlation_object1, correlation_object2],
     *        payloadData: [object1, object2, object3],
     *        arbitraryDataMap: mapObject,
     *    }
     */
    @Test
    public void testWSO2InputMappingDefault() throws InterruptedException {
        log.info("Test case for wso2event input mapping with mapping with meta, correlation, payload and arbitrary " +
                "values");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='wso2event', " +
                                                                "arbitrary.map='arbitrary_object')) " +
                "define stream FooStream (meta_timestamp long, correlation_symbol string, symbol string, " +
                                        "price float, volume int, arbitrary_object object); " +
                "define stream BarStream (meta_timestamp long, correlation_symbol string, symbol string, " +
                                        "price float, volume int, arbitrary_object object); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            org.junit.Assert.assertEquals(1496814501L, event.getData(0));
                            break;
                        case 2:
                            org.junit.Assert.assertEquals("IBM", event.getData(1));
                            break;
                        case 3:
                            org.junit.Assert.assertEquals(102.5f, event.getData(3));
                            Map<String, String> arbitraryDataMap = (Map<String, String>) event.getData(5);
                            org.junit.Assert.assertEquals("value111", arbitraryDataMap.get("key111"));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", wso2event);
        InMemoryBroker.publish("stock", wso2event1);
        InMemoryBroker.publish("stock", wso2event2);
        //assert event count
        Assert.assertEquals("Number of events", 3, count.get());
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testWSO2InputMappingDefaultWithoutStreamId() throws InterruptedException {
        log.info("Test case for wso2event input mapping with mapping with meta, correlation, payload and arbitrary " +
                "values WITHOUT mentioning wso2.stream.id");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='wso2event', arbitrary.map='arbitrary_object')) " +
                "define stream FooStream (meta_timestamp long, correlation_symbol string, symbol string, " +
                                        "price float, volume int, arbitrary_object object); " +
                "define stream BarStream (meta_timestamp long, correlation_symbol string, symbol string, " +
                                        "price float, volume int, arbitrary_object object); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            org.junit.Assert.assertEquals(1496814501L, event.getData(0));
                            break;
                        case 2:
                            org.junit.Assert.assertEquals("IBM", event.getData(1));
                            break;
                        case 3:
                            org.junit.Assert.assertEquals(102.5f, event.getData(3));
                            Map<String, String> arbitraryDataMap = (Map<String, String>) event.getData(5);
                            org.junit.Assert.assertEquals("value111", arbitraryDataMap.get("key111"));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        wso2event.setStreamId("FooStream:1.0.0");
        InMemoryBroker.publish("stock", wso2event);
        wso2event1.setStreamId("FooStream:1.0.0");
        InMemoryBroker.publish("stock", wso2event1);
        wso2event2.setStreamId("FooStream:1.0.0");
        InMemoryBroker.publish("stock", wso2event2);
        //assert event count
        Assert.assertEquals("Number of events", 3, count.get());
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }
}
