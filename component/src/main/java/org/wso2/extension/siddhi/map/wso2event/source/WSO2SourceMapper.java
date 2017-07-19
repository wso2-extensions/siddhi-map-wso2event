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

package org.wso2.extension.siddhi.map.wso2event.source;

import org.apache.log4j.Logger;
import org.wso2.carbon.databridge.commons.exception.MalformedStreamDefinitionException;
import org.wso2.extension.siddhi.map.wso2event.WSO2EventMapperUtils;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.source.AttributeMapping;
import org.wso2.siddhi.core.stream.input.source.InputEventHandler;
import org.wso2.siddhi.core.stream.input.source.SourceMapper;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.wso2.extension.siddhi.map.wso2event.WSO2EventMapperUtils.ARBITRARY_DATA_PREFIX;
import static org.wso2.extension.siddhi.map.wso2event.WSO2EventMapperUtils.CORRELATION_DATA_PREFIX;
import static org.wso2.extension.siddhi.map.wso2event.WSO2EventMapperUtils.META_DATA_PREFIX;

/**
 * This mapper converts WSO2 input event to {@link org.wso2.siddhi.core.event.ComplexEventChunk}. This extension
 * accepts WSO2 events from WSO2 receiver and maps attribute values to the defined stream.
 */
@Extension(
        name = "wso2event",
        namespace = "sourceMapper",
        description = "WSO2 Event to Siddhi Event input mapper. Transports which accepts WSO2 messages can utilize " +
                "this extension to convert the incoming WSO2 message to Siddhi event. Users can send a WSO2 message" +
                "which should contain the mapping stream attributes in the same order as the defined stream (Within " +
                "a WSO2 Event attribute type). This conversion will happen without any configs.\n" +
                "Following prefixes will be used to identify different attributes such as meta, correlation, payload " +
                "and arbitrary. Prefixes available," +
                "1. meta_ - metaData," +
                "2. correlation_ - correlationData," +
                "3. arbitrary_ - value contained in the arbitraryMap mapped to the key defined after the prefix" +
                "If the above prefixes are not used, the attribute is taken as payload data.",
        examples = {
                @Example(
                        syntax = "@source(type=’wso2event’, @map(type=’wso2event’) " +
                                "define stream FooStream (meta_timestamp long, symbol string, price float, " +
                                "volume long);",
                        description = "Above configuration will perform a WSO2 default mapping. Expected input " +
                                "should be as follows.\n" +
                                "Wso2event = {\n" +
                                "                streamId: org.wso2event.fooStream:1.0.0,\n" +
                                "                timestamp: 431434134134,\n" +
                                "                metaData: [timestamp],\n" +
                                "                correlationData: [],\n" +
                                "                payloadData: [symbol, price, volume]\n" +
                                "            }\n"),
                @Example(
                        syntax = "@source(type=’wso2event’, @map(type=’wso2event’, " +
                                "@attributes(" +
                                "timestamp='meta_timestamp'," +
                                "symbol='symbol'," +
                                "price='price'," +
                                "volume='volume'," +
                                "portfolioId='arbitrary_portfolio_ID'" +
                                ")))" +
                                "define stream FooStream (meta_timestamp long, symbol string, price float, " +
                                "volume long, portfolioId string)); ",
                        description = "Above configuration will perform a WSO2 mapping. Expected input should be as " +
                                "follows,\n" +
                                "Wso2event = {\n" +
                                "                streamId: org.wso2event.fooStream:1.0.0,\n" +
                                "                timeStamp: 431434134134,\n" +
                                "                metaData: [timestamp],\n" +
                                "                correlationData: [],\n" +
                                "                payloadData: [symbol, price, volume],\n" +
                                "                arbitraryDataMap: objectMap,\n" +
                                "            }\n" +
                                "The WSO2 mapper will get the arbitrary map in the WSO2 event and assign its' " +
                                "value associated with the key 'portfolio_ID' to the siddhi attribute. If the map " +
                                "does not contain the key value pair, null will be passed."),
        }
)
public class WSO2SourceMapper extends SourceMapper {
    private static final Logger LOGGER = Logger.getLogger(WSO2SourceMapper.class);

    private org.wso2.carbon.databridge.commons.StreamDefinition streamDefinition;
    private int numAttributes;
    private Map<Integer, Integer> metaDataMap;
    private Map<Integer, Integer> correlationDataMap;
    private Map<Integer, Integer> payloadDataMap;
    private Map<String, Integer> arbitraryDataMap;

    /**
     * Initialize the mapper and the mapping configurations.
     *
     * @param streamDefinition the StreamDefinition
     * @param optionHolder     mapping options
     * @param list             list of attributes mapping
     * @param configReader     Deployment Config Reader
     * @param siddhiAppContext Siddhi App context
     */
    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, List<AttributeMapping> list,
                     ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        List<Attribute> attributeList = streamDefinition.getAttributeList();

        this.numAttributes = attributeList.size();
        this.metaDataMap = new HashMap<>();
        this.correlationDataMap = new HashMap<>();
        this.payloadDataMap = new HashMap<>();
        this.arbitraryDataMap = new HashMap<>();

        int metaCount = 0, correlationCount = 0, payloadCount = 0;
        List<org.wso2.carbon.databridge.commons.Attribute> metaAttributeList = new ArrayList<>();
        List<org.wso2.carbon.databridge.commons.Attribute> correlationAttributeList = new ArrayList<>();
        List<org.wso2.carbon.databridge.commons.Attribute> payloadAttributeList = new ArrayList<>();

        for (int i = 0; i < attributeList.size(); i++) {
            String attributeName;
            if (list != null && list.size() > 0) {
                attributeName = list.get(i).getMapping();
            } else {
                attributeName = attributeList.get(i).getName();
            }
            Attribute.Type attributeType = attributeList.get(i).getType();

            if (attributeName.startsWith(META_DATA_PREFIX)) {
                //meta array's metaCount'th attribute of import stream will be mapped to the i'th
                // location of the export stream.
                metaAttributeList.add(WSO2EventMapperUtils.createWso2EventAttribute(attributeName, attributeType));
                this.metaDataMap.put(metaCount, i);
                metaCount++;
            } else if (attributeName.startsWith(CORRELATION_DATA_PREFIX)) {
                correlationAttributeList.add(WSO2EventMapperUtils
                        .createWso2EventAttribute(attributeName, attributeType));
                this.correlationDataMap.put(correlationCount, i);
                correlationCount++;
            } else if (attributeName.startsWith(ARBITRARY_DATA_PREFIX)) {
                if (attributeType.equals(Attribute.Type.STRING)) {
                    this.arbitraryDataMap.put(attributeName.replace(ARBITRARY_DATA_PREFIX, ""), i);
                } else {
                    throw new SiddhiAppCreationException("Arbitrary Map attribute '" + attributeName + "' has " +
                            "been mapped to '" + attributeType + "' in Siddhi app '" + siddhiAppContext.getName() +
                            "'. However, arbitrary map value can only be mapped to type 'String'.");
                }
            } else {
                payloadAttributeList.add(WSO2EventMapperUtils.createWso2EventAttribute(attributeName, attributeType));
                this.payloadDataMap.put(payloadCount, i);
                payloadCount++;
            }
        }

        try {
            this.streamDefinition = WSO2EventMapperUtils.createWSO2EventStreamDefinition(streamDefinition.getId(),
                    metaAttributeList, correlationAttributeList, payloadAttributeList);
        } catch (MalformedStreamDefinitionException e) {
            throw new SiddhiAppValidationException(e.getMessage(), e);
        }
    }

    /**
     * Receives an event as an WSO2 event from WSO2 Receiver {@link org.wso2.siddhi.core.stream.input.source.Source},
     * converts it to a {@link org.wso2.siddhi.core.event.ComplexEventChunk} and send to the
     * {@link org.wso2.siddhi.core.query.output.callback.OutputCallback}.
     *
     * @param eventObject       the input event, given as an WSO2 event object
     * @param inputEventHandler input handler
     */
    @Override
    protected void mapAndProcess(Object eventObject, InputEventHandler inputEventHandler) throws InterruptedException {
        org.wso2.carbon.databridge.commons.Event wso2event;
        if (eventObject instanceof org.wso2.carbon.databridge.commons.Event) {
            wso2event = (org.wso2.carbon.databridge.commons.Event) eventObject;
            Object outputAttributes[] = new Object[this.numAttributes];

            for (Map.Entry<Integer, Integer> entry : this.metaDataMap.entrySet()) {
                outputAttributes[entry.getValue()] = wso2event.getMetaData()[entry.getKey()];
            }

            for (Map.Entry<Integer, Integer> entry : this.correlationDataMap.entrySet()) {
                outputAttributes[entry.getValue()] = wso2event.getCorrelationData()[entry.getKey()];
            }

            for (Map.Entry<Integer, Integer> entry : this.payloadDataMap.entrySet()) {
                outputAttributes[entry.getValue()] = wso2event.getPayloadData()[entry.getKey()];
            }

            for (Map.Entry<String, Integer> entry : this.arbitraryDataMap.entrySet()) {
                outputAttributes[entry.getValue()] = wso2event.getArbitraryDataMap().get(entry.getKey());
            }

            inputEventHandler.sendEvent(new Event(wso2event.getTimeStamp(), outputAttributes));
        } else {
            LOGGER.warn("Event object is invalid. Expected WSO2Event, but found " + eventObject.getClass()
                    .getCanonicalName() + ". Hence, dropping the event");
        }
    }

    public org.wso2.carbon.databridge.commons.StreamDefinition getWSO2StreamDefinition() {
        return this.streamDefinition;
    }

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{org.wso2.carbon.databridge.commons.Event.class};
    }

}
