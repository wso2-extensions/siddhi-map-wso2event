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

package io.sidhi.extension.map.wso2event;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.transport.InMemoryBroker;
import io.siddhi.core.util.transport.SubscriberUnAvailableException;
import org.apache.log4j.Logger;

import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class WSO2SourceMapperTestCase {
    private static final Logger log = Logger.getLogger(WSO2SourceMapperTestCase.class);
    private AtomicInteger count = new AtomicInteger();
    private org.wso2.carbon.databridge.commons.Event wso2event;
    private org.wso2.carbon.databridge.commons.Event wso2event1;
    private org.wso2.carbon.databridge.commons.Event wso2event2;
    private org.wso2.carbon.databridge.commons.Event wso2event3;
    private org.wso2.carbon.databridge.commons.Event wso2event4;
    private org.wso2.carbon.databridge.commons.Event wso2event5;
    private org.wso2.carbon.databridge.commons.Event wso2event6;
    private org.wso2.carbon.databridge.commons.Event wso2eventWithoutPayloadAttribute;

    @BeforeMethod
    public void init() {
        count.set(0);
        Object meta[] = {1496814501L};
        Object correlation[] = {"IBM", 500.5f, 500};
        Object payload[] = {"WSO2", 100.5f, 50};
        Map<String, String> arbitraryDataMap = new HashMap<>();
        arbitraryDataMap.put("key1", "value1");
        arbitraryDataMap.put("key2", "value2");
        wso2event = new org.wso2.carbon.databridge.commons.Event("org" +
                ".fooStream:1.0.0", 1496814773L, meta, correlation, payload, arbitraryDataMap);

        Object meta1[] = {1496815144L};
        Object correlation1[] = {"IBM", 500.5f, 500};
        Object payload1[] = {"WSO22", 101.5f, 51};
        Map<String, String> arbitraryDataMap1 = new HashMap<>();
        arbitraryDataMap1.put("key1", "value11");
        arbitraryDataMap1.put("key2", "value22");
        wso2event1 = new org.wso2.carbon.databridge.commons.Event("org" +
                ".fooStream:1.0.0", 1496815276L, meta1, correlation1, payload1, arbitraryDataMap1);

        Object meta2[] = {1496815144L};
        Object correlation2[] = {"IBM", 500.5f, 500};
        Object payload2[] = {"WSO222", 102.5f, 52};
        Map<String, String> arbitraryDataMap2 = new HashMap<>();
        arbitraryDataMap2.put("key1", "value111");
        arbitraryDataMap2.put("key2", "value222");
        wso2event2 = new org.wso2.carbon.databridge.commons.Event("org" +
                ".fooStream:1.0.0", 1496815276L, meta2, correlation2, payload2, arbitraryDataMap2);

        wso2event3 = new org.wso2.carbon.databridge.commons.Event("org.fooStream:1.0.0",
                1496815276L, null, correlation2, payload2, null);

        wso2event4 = new org.wso2.carbon.databridge.commons.Event("org.fooStream:1.0.0",
                1496815276L, meta2, null, payload2, null);

        wso2event5 = new org.wso2.carbon.databridge.commons.Event("org.fooStream:1.0.0",
                1496815276L, null, null, payload2, arbitraryDataMap2);

        Object meta3[] = {1496815144L, 14968333333L};
        wso2event6 = new org.wso2.carbon.databridge.commons.Event("org.fooStream:1.0.0",
                1496815276L, meta3, correlation2, payload2, arbitraryDataMap2);


        wso2eventWithoutPayloadAttribute = new org.wso2.carbon.databridge.commons.Event("org" +
                ".fooStream:1.0.0", 1496814773L, meta, correlation, null, arbitraryDataMap);
    }

    /**
     * Expected input format:
     * Wso2event = {
     * streamId: wso2event.fooStream:1.0.0,
     * timeStamp: 431434134134,
     * metaData: [meta_object1, meta_object2],
     * correlationData: [correlation_object1, correlation_object2],
     * payloadData: [object1, object2, object3],
     * arbitraryDataMap: mapObject,
     * }
     */
    @Test
    public void testWSO2InputMappingDefault() throws InterruptedException, SubscriberUnAvailableException {
        log.info("Test case for wso2event input mapping with mapping with meta, correlation, payload and arbitrary " +
                "values");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='wso2event')) " +
                "define stream FooStream (meta_timestamp long, correlation_symbol string, symbol string, " +
                "price float, volume int, arbitrary_key1 string); " +
                "define stream BarStream (meta_timestamp long, correlation_symbol string, symbol string, " +
                "price float, volume int, arbitrary_key1 string); ";
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
                            AssertJUnit.assertEquals(1496814501L, event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("IBM", event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(102.5f, event.getData(3));
                            AssertJUnit.assertEquals("value111", event.getData(5));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", wso2event);
        InMemoryBroker.publish("stock", wso2event1);
        InMemoryBroker.publish("stock", wso2event2);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, count.get());
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testWSO2InputMappingDefaultWithCorrelationAttribute() throws InterruptedException,
                                                                             SubscriberUnAvailableException {
        log.info("Test case for wso2event input mapping with mapping with correlation and payload " +
                "values");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='wso2event')) " +
                "define stream FooStream (correlation_symbol string, symbol string, price float, volume int); " +
                "define stream BarStream (correlation_symbol string, symbol string, price float, volume int); ";
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
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", wso2event3);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testWSO2InputMappingDefaultWithMetaAttribute() throws InterruptedException,
                                                                      SubscriberUnAvailableException {
        log.info("Test case for wso2event input mapping with mapping with meta and payload " +
                "values");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='wso2event')) " +
                "define stream FooStream (meta_timestamp long, symbol string, price float, volume int); " +
                "define stream BarStream (meta_timestamp long, symbol string, price float, volume int); ";
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
                            AssertJUnit.assertEquals(1496815144L, event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", wso2event4);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testWSO2InputMappingDefaultWithArbitraryAttributes() throws InterruptedException,
                                                                            SubscriberUnAvailableException {
        log.info("Test case for wso2event input mapping with mapping with payload and arbitrary " +
                "values");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='wso2event')) " +
                "define stream FooStream (symbol string, price float, volume int, arbitrary_key1 string); " +
                "define stream BarStream (symbol string, price float, volume int, arbitrary_key1 string); ";
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
                            AssertJUnit.assertEquals(102.5f, event.getData(1));
                            AssertJUnit.assertEquals("value111", event.getData(3));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", wso2event5);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testWSO2InputMappingDefaultWithMultiArbitraryAttributes() throws InterruptedException {
        log.info("Test case for wso2event input mapping with mapping with payload and arbitrary " +
                "values");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='wso2event')) " +
                "define stream FooStream (symbol string, price float, volume int, arbitrary_key1 string, " +
                "arbitrary_key2 float); ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testWSO2InputMappingDefaultWithArbitraryAttributesOtherThanString() throws
                                                                                    SubscriberUnAvailableException {
        log.info("Test case for wso2event input mapping with mapping with payload and arbitrary " +
                "values");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='wso2event')) " +
                "define stream FooStream (symbol string, price float, volume int, arbitrary_key1 string, " +
                "arbitrary_key2 string); " +
                "define stream BarStream (symbol string, price float, volume int, arbitrary_key1 string, " +
                "arbitrary_key2 string); ";
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
                            AssertJUnit.assertEquals(102.5f, event.getData(1));
                            AssertJUnit.assertEquals("value111", event.getData(3));
                            AssertJUnit.assertEquals("value222", event.getData(4));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", wso2event5);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testWSO2InputMappingDefaultWithMultiMetaAttributes() throws InterruptedException,
                                                                            SubscriberUnAvailableException {
        log.info("Test case for wso2event input mapping with mapping with meta, correlation, payload and arbitrary " +
                "values");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='wso2event')) " +
                "define stream FooStream (meta_timestamp long, meta_timestamp2 long, correlation_symbol string, " +
                "symbol string, price float, volume int, arbitrary_key1 string); " +
                "define stream BarStream (meta_timestamp long, meta_timestamp2 long, correlation_symbol string, " +
                "symbol string, price float, volume int, arbitrary_key1 string); ";
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
                            AssertJUnit.assertEquals(1496815144L, event.getData(0));
                            AssertJUnit.assertEquals(14968333333L, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", wso2event6);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testWSO2InputMappingDefaultWithoutPayloadAttribute() throws InterruptedException,
                                                                            SubscriberUnAvailableException {
        log.info("Test case for wso2event input mapping with mapping without payload values " +
                "values");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='wso2event')) " +
                "define stream FooStream (meta_timestamp long, correlation_symbol string, arbitrary_key1 string); " +
                "define stream BarStream (meta_timestamp long, correlation_symbol string, arbitrary_key1 string); ";

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
                            AssertJUnit.assertEquals(1496814501L, event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("IBM", event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("value111", event.getData(3));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", wso2eventWithoutPayloadAttribute);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testWSO2InputMappingDefaultForSingleEvent() throws SubscriberUnAvailableException {
        log.info("Test case for wso2event input mapping with mapping with meta, correlation, payload and arbitrary " +
                "values");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='wso2event')) " +
                "define stream FooStream (meta_timestamp long, correlation_symbol string, symbol string, " +
                "price float, volume int, arbitrary_key1 string); " +
                "define stream BarStream (meta_timestamp long, correlation_symbol string, symbol string, " +
                "price float, volume int, arbitrary_key1 string); ";
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
                            AssertJUnit.assertEquals(1496814501L, event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", wso2event);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }


    @Test
    public void testWSO2InputMappingDefaultWithoutStreamId() throws InterruptedException,
                                                                    SubscriberUnAvailableException {
        log.info("Test case for wso2event input mapping with mapping with meta, correlation, payload and arbitrary " +
                "values");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='wso2event')) " +
                "define stream FooStream (meta_timestamp long, correlation_symbol string, symbol string, " +
                "price float, volume int, arbitrary_key1 string); " +
                "define stream BarStream (meta_timestamp long, correlation_symbol string, symbol string, " +
                "price float, volume int, arbitrary_key1 string); ";
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
                            AssertJUnit.assertEquals(1496814501L, event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("IBM", event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(102.5f, event.getData(3));
                            AssertJUnit.assertEquals("value111", event.getData(5));
                            break;
                        default:
                            AssertJUnit.fail();
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
        AssertJUnit.assertEquals("Number of events", 3, count.get());
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testWSO2InputMappingCustom() throws InterruptedException, SubscriberUnAvailableException {
        log.info("Test case for wso2event input mapping with mapping with meta, correlation, payload and arbitrary " +
                "values");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='wso2event', " +
                "@attributes(" +
                "timestamp = 'meta_timestamp'," +
                "symbolCR = 'correlation_symbol'," +
                "symbol = 'symbol'," +
                "key1 = 'arbitrary_key1'," +
                "price ='price'," +
                "volume = 'volume'" +
                "))) " +
                "define stream FooStream (timestamp long, symbolCR string, symbol string, " +
                "price float, volume int, key1 string); " +
                "define stream BarStream (timestamp long, symbolCR string, symbol string, " +
                "price float, volume int, key1 string); ";
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
                            AssertJUnit.assertEquals(1496814501L, event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("IBM", event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(102.5f, event.getData(3));
                            AssertJUnit.assertEquals("value111", event.getData(5));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", wso2event);
        InMemoryBroker.publish("stock", wso2event1);
        InMemoryBroker.publish("stock", wso2event2);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, count.get());
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

}
