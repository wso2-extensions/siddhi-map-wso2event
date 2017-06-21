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
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.sink.InMemorySink;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class WSO2SinkMapperTestCase {
    private static final Logger log = Logger.getLogger(WSO2SinkMapperTestCase.class);
    private AtomicInteger wso2Count = new AtomicInteger(0);
    private AtomicInteger ibmCount = new AtomicInteger(0);

    private Map<String, String> arbitraryDataMap = new HashMap<>();
    private Map<String, String> arbitraryDataMap1 = new HashMap<>();
    private Map<String, String> arbitraryDataMap2 = new HashMap<>();

    @Before
    public void init() {
        wso2Count.set(0);
        ibmCount.set(0);
        arbitraryDataMap.put("key1", "value1");
        arbitraryDataMap.put("key2", "value2");
        arbitraryDataMap1.put("key11", "value11");
        arbitraryDataMap1.put("key22", "value22");
        arbitraryDataMap2.put("key111", "value111");
        arbitraryDataMap2.put("key222", "value222");
    }

    @Test
    public void testWSO2SinkmapperDefaultMappingWithSiddhiQL() throws InterruptedException {
        log.info("Test default wso2event mapping with SiddhiQL");
        List<Object> onMessageList = new ArrayList<Object>();
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (meta_timestamp long, correlation_symbol string, symbol string, price float," +
                " volume int, arbitrary_object object); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='wso2event', " +
                "arbitrary.map='arbitrary_object')) " +
                "define stream BarStream (meta_timestamp long, correlation_symbol string, symbol string, price float," +
                " volume int, arbitrary_object object); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("sink:inMemory", InMemorySink.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{1212212121L, "Lanka", "WSO2", 55.645f, 100L, arbitraryDataMap});
        stockStream.send(new Object[]{2212212121L, "US", "IBM", 65.645f, 200L, arbitraryDataMap1});
        stockStream.send(new Object[]{3212212121L, "SL", "IBM", 75.645f, 300L});
        stockStream.send(new Object[]{3212212121L, "SL", "WSO2", null, 300L, arbitraryDataMap2});
        Thread.sleep(100);

        //assert event count
        Assert.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        Assert.assertEquals("Incorrect number of events consumed!", 2, ibmCount.get());

        org.wso2.carbon.databridge.commons.Event wso2event =
                (org.wso2.carbon.databridge.commons.Event) onMessageList.get(0);
        org.junit.Assert.assertEquals(1212212121L, wso2event.getMetaData()[0]);
        org.junit.Assert.assertEquals("Lanka", wso2event.getCorrelationData()[0]);
        org.junit.Assert.assertEquals("WSO2", wso2event.getPayloadData()[0]);
        org.junit.Assert.assertEquals("BarStream:1.0.0", wso2event.getStreamId());

        wso2event = (org.wso2.carbon.databridge.commons.Event) onMessageList.get(1);
        org.junit.Assert.assertEquals(65.645f, wso2event.getPayloadData()[1]);
        org.junit.Assert.assertEquals(200L, wso2event.getPayloadData()[2]);
        Map<String, String> arbitraryObject = wso2event.getArbitraryDataMap();
        org.junit.Assert.assertEquals("value11", arbitraryObject.get("key11"));

        wso2event = (org.wso2.carbon.databridge.commons.Event) onMessageList.get(2);
        org.junit.Assert.assertEquals(null, wso2event.getArbitraryDataMap());

        wso2event = (org.wso2.carbon.databridge.commons.Event) onMessageList.get(3);
        org.junit.Assert.assertEquals(null, wso2event.getPayloadData()[1]);
        arbitraryObject = wso2event.getArbitraryDataMap();
        org.junit.Assert.assertEquals("value222", arbitraryObject.get("key222"));

        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }

    @Test
    public void testWSO2SinkmapperWithoutToStreamIDDefaultMappingWithSiddhiQL() throws InterruptedException {
        log.info("Test default wso2event mapping without WSO2 event stream name defined in query with SiddhiQL");
        List<Object> onMessageList = new ArrayList<Object>();
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (meta_timestamp long, correlation_symbol string, symbol string, price float," +
                " volume int, arbitrary_object object); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='wso2event', " +
                "arbitrary.map='arbitrary_object')) " +
                "define stream BarStream (meta_timestamp long, correlation_symbol string, symbol string, price float," +
                " volume int, arbitrary_object object); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("sink:inMemory", InMemorySink.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{1212212121L, "Lanka", "WSO2", 55.645f, 100L, arbitraryDataMap});
        stockStream.send(new Object[]{2212212121L, "US", "IBM", 65.645f, 200L, arbitraryDataMap1});
        stockStream.send(new Object[]{3212212121L, "SL", "IBM", 75.645f, 300L});
        stockStream.send(new Object[]{3212212121L, "SL", "WSO2", null, 300L, arbitraryDataMap2});
        Thread.sleep(100);

        //assert event count
        Assert.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        Assert.assertEquals("Incorrect number of events consumed!", 2, ibmCount.get());

        org.wso2.carbon.databridge.commons.Event wso2event =
                (org.wso2.carbon.databridge.commons.Event) onMessageList.get(0);
        org.junit.Assert.assertEquals(1212212121L, wso2event.getMetaData()[0]);
        org.junit.Assert.assertEquals("Lanka", wso2event.getCorrelationData()[0]);
        org.junit.Assert.assertEquals("WSO2", wso2event.getPayloadData()[0]);
        org.junit.Assert.assertEquals("BarStream:1.0.0", wso2event.getStreamId());

        wso2event = (org.wso2.carbon.databridge.commons.Event) onMessageList.get(1);
        org.junit.Assert.assertEquals(65.645f, wso2event.getPayloadData()[1]);
        org.junit.Assert.assertEquals(200L, wso2event.getPayloadData()[2]);
        Map<String, String> arbitraryObject = wso2event.getArbitraryDataMap();
        org.junit.Assert.assertEquals("value11", arbitraryObject.get("key11"));

        wso2event = (org.wso2.carbon.databridge.commons.Event) onMessageList.get(2);
        org.junit.Assert.assertEquals(null, wso2event.getArbitraryDataMap());

        wso2event = (org.wso2.carbon.databridge.commons.Event) onMessageList.get(3);
        org.junit.Assert.assertEquals(null, wso2event.getPayloadData()[1]);
        arbitraryObject = wso2event.getArbitraryDataMap();
        org.junit.Assert.assertEquals("value222", arbitraryObject.get("key222"));

        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }

    @Test
    public void testWSO2SinkmapperWithMixedAttributeTypesMappingWithSiddhiQL() throws InterruptedException {
        log.info("Test default wso2event mapping when the attributes (types: meta, correlation, payload) are defined " +
                "out of order in query with SiddhiQL");
        List<Object> onMessageList = new ArrayList<Object>();
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (meta_timestamp long, symbol string, correlation_symbol string, price float," +
                " arbitrary_object object, volume int); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='wso2event', " +
                "arbitrary.map='arbitrary_object')) " +
                "define stream BarStream (meta_timestamp long, symbol string, correlation_symbol string, price float," +
                " arbitrary_object object, volume int); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("sink:inMemory", InMemorySink.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{1212212121L, "WSO2", "Lanka", 55.645f, arbitraryDataMap, 100L});
        stockStream.send(new Object[]{2212212121L, "IBM", "US", 65.645f, arbitraryDataMap1, 200L});
        stockStream.send(new Object[]{3212212121L, "IBM", "SL", 75.645f, null, 300L});
        stockStream.send(new Object[]{3212212121L, "WSO2", "SL", null, arbitraryDataMap2, 300L});
        Thread.sleep(100);

        //assert event count
        Assert.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        Assert.assertEquals("Incorrect number of events consumed!", 2, ibmCount.get());

        org.wso2.carbon.databridge.commons.Event wso2event =
                (org.wso2.carbon.databridge.commons.Event) onMessageList.get(0);
        org.junit.Assert.assertEquals(1212212121L, wso2event.getMetaData()[0]);
        org.junit.Assert.assertEquals("Lanka", wso2event.getCorrelationData()[0]);
        org.junit.Assert.assertEquals("WSO2", wso2event.getPayloadData()[0]);
        org.junit.Assert.assertEquals("BarStream:1.0.0", wso2event.getStreamId());

        wso2event = (org.wso2.carbon.databridge.commons.Event) onMessageList.get(1);
        org.junit.Assert.assertEquals(65.645f, wso2event.getPayloadData()[1]);
        org.junit.Assert.assertEquals(200L, wso2event.getPayloadData()[2]);
        Map<String, String> arbitraryObject = wso2event.getArbitraryDataMap();
        org.junit.Assert.assertEquals("value11", arbitraryObject.get("key11"));

        wso2event = (org.wso2.carbon.databridge.commons.Event) onMessageList.get(2);
        org.junit.Assert.assertEquals(null, wso2event.getArbitraryDataMap());
        org.junit.Assert.assertEquals(300L, wso2event.getPayloadData()[2]);

        wso2event = (org.wso2.carbon.databridge.commons.Event) onMessageList.get(3);
        org.junit.Assert.assertEquals(null, wso2event.getPayloadData()[1]);
        arbitraryObject = wso2event.getArbitraryDataMap();
        org.junit.Assert.assertEquals("value222", arbitraryObject.get("key222"));

        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }
}
