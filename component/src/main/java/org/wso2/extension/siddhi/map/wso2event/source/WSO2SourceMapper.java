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
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.AttributeMapping;
import org.wso2.siddhi.core.stream.input.InputEventHandler;
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
import java.util.TreeMap;

/**
 * This mapper converts WSO2 input event to {@link org.wso2.siddhi.core.event.ComplexEventChunk}. This extension
 * accepts WSO2 events from WSO2 receiver and maps attribute values to the defined stream.
 */
@Extension(
        name = "wso2event",
        namespace = "sourceMapper",
        description = "WSO2 event to Siddhi Event input mapper. Transports which accepts WSO2 messages can utilize " +
                "this extension to convert the incoming WSO2 message to Siddhi event. Users can send a WSO2 message" +
                "which should contain the mapping stream attributes in the same order as the defined stream. This " +
                "conversion will happen without any configs.",
        parameters = {
                @Parameter(name = "arbitrary.map",
                        description =
                                "Used to provide the attribute name of the stream which the arbitrary object to be " +
                                        "mapped" +
                                        "eg: arbitrary.map='foo' foo is a attribute name in the stream definition " +
                                        "with the attribute type object",
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "@source(type=’wso2event’, @map(type=’wso2event’) " +
                                "define stream FooStream (meta_timestamp long, symbol string, price float, " +
                                "volume long);",
                        description = "Above configuration will do a WSO2 mapping. Expected input will look like " +
                                "below." +
                                "Wso2event = {" +
                                "                streamId: org.wso2event.fooStream:1.0.0,\n" +
                                "                timeStamp: 431434134134,\n" +
                                "                metaData: [timestamp, meta_object2],\n" +
                                "                correlationData: [correlation_object1],\n" +
                                "                payloadData: [symbol, price, volume]\n" +
                                "            }" +
                                "There can be at least the number of attributes of each type (meta, " +
                                "correlation, payload) or more than defined in the stream definition" +
                                " eg: metaData array has more than meta attributes defined and payloadData " +
                                "has the exact amount of attributes as defined in the stream"),
                @Example(
                        syntax = "@source(type=’wso2event’, @map(type=’wso2event’, arbitrary.map='arbitrary_object'))" +
                                "define stream FooStream (meta_timestamp long, symbol string, price float, " +
                                "volume long, arbitrary_object object)); ",
                        description = "Above configuration will do a WSO2 mapping which also expects an arbitrary " +
                                "map. Expected input will look like below." +
                                "Wso2event = {" +
                                "                streamId: org.wso2event.fooStream:1.0.0,\n" +
                                "                timeStamp: 431434134134,\n" +
                                "                metaData: [timestamp, meta_object2],\n" +
                                "                correlationData: [correlation_object1],\n" +
                                "                payloadData: [symbol, price, volume],\n" +
                                "                arbitraryDataMap: objectMap,\n" +
                                "            }" +
                                "The WSO2 mapper will get the arbitrary map in the WSO2 event. And assign its" +
                                " value. If the map is not defined, the Siddhi events arbitrary object value " +
                                "would be assigned as null"),
        }
)
public class WSO2SourceMapper extends SourceMapper {
    private static final Logger LOGGER = Logger.getLogger(WSO2SourceMapper.class);
    private List<Attribute> attributeList;
    private Map<WSO2EventMapperUtils.InputDataType, Map<Integer, Integer>> attributePositionMap = null;
    private int arbitraryAttributeIndex = -1;
    private org.wso2.carbon.databridge.commons.StreamDefinition streamDefinition;

    /**
     * Initialize the mapper and the mapping configurations.
     *
     * @param streamDefinition     the StreamDefinition
     * @param optionHolder         mapping options
     * @param attributeMappingList list of attributes mapping
     * @param configReader
     */
    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, List<AttributeMapping>
            attributeMappingList, ConfigReader configReader) {
        attributeList = streamDefinition.getAttributeList();

        //TODO - convert attributePositionMap in to list, loop through that and set values. Update : Seems like this is
        // bit expensive operation compare to the current one then avoided it.

        attributePositionMap = new HashMap<>(attributeList.size());
        String arbitraryAttributeName = optionHolder.validateAndGetStaticValue(
                WSO2EventMapperUtils.ARBITRARY_MAP_ATTRIBUTE_PARAMETER_NAME, null);
        Map<Integer, Integer> payloadDataMap = new TreeMap<Integer, Integer>();
        Map<Integer, Integer> metaDataMap = new TreeMap<Integer, Integer>();
        Map<Integer, Integer> correlationDataMap = new TreeMap<Integer, Integer>();
        int metaCount = 0, correlationCount = 0, payloadCount = 0;
        List<org.wso2.carbon.databridge.commons.Attribute> metaAttributeList = new ArrayList<>();
        List<org.wso2.carbon.databridge.commons.Attribute> correlationAttributeList = new ArrayList<>();
        List<org.wso2.carbon.databridge.commons.Attribute> payloadAttributeList = new ArrayList<>();
        org.wso2.carbon.databridge.commons.Attribute wso2eventAttribute;
        if (attributeMappingList != null && attributeMappingList.size() > 0) {
            throw new SiddhiAppValidationException("WSO2 Transport does not support custom mapping. Please remove" +
                    " @attributes section in mapping.");
        } else {
            //default mapping scenario
            for (int i = 0; i < attributeList.size(); i++) {
                if (attributeList.get(i).getName().startsWith(WSO2EventMapperUtils.META_DATA_PREFIX)) {
                    //meta array's metaCount'th attribute of import stream will be mapped to the i'th
                    // location of the export stream.
                    wso2eventAttribute = WSO2EventMapperUtils.createWso2EventAttribute(attributeList.get(i));
                    if (null != wso2eventAttribute) {
                        metaAttributeList.add(wso2eventAttribute);
                    }
                    metaDataMap.put(metaCount, i);
                    metaCount++;
                } else if (attributeList.get(i).getName().startsWith(WSO2EventMapperUtils.CORRELATION_DATA_PREFIX)) {
                    wso2eventAttribute = WSO2EventMapperUtils.createWso2EventAttribute(attributeList.get(i));
                    if (null != wso2eventAttribute) {
                        correlationAttributeList.add(wso2eventAttribute);
                    }
                    correlationDataMap.put(correlationCount, i);
                    correlationCount++;
                } else if (null != arbitraryAttributeName &&
                        attributeList.get(i).getName().equals(arbitraryAttributeName)) {
                    if (Attribute.Type.OBJECT.equals(attributeList.get(i).getType())) {
                        arbitraryAttributeIndex = i;
                    } else {
                        throw new SiddhiAppValidationException("defined arbitrary.map attribute in the " +
                                "stream mapping is type: " + attributeList.get(i).getType() + ". It should be type: " +
                                Attribute.Type.OBJECT);
                    }
                } else if (Attribute.Type.OBJECT.equals(attributeList.get(i).getType())) {
                    throw new SiddhiAppValidationException("Please define arbitrary.map attribute in the " +
                            "stream mapping if there is a \"object\" type attribute in the stream definition");
                } else {
                    wso2eventAttribute = WSO2EventMapperUtils.createWso2EventAttribute(attributeList.get(i));
                    if (null != wso2eventAttribute) {
                        payloadAttributeList.add(wso2eventAttribute);
                    }
                    payloadDataMap.put(payloadCount, i);
                    payloadCount++;
                }
            }
        }
        try {
            this.streamDefinition = WSO2EventMapperUtils.createWSO2EventStreamDefinition(streamDefinition.getId(),
                    metaAttributeList, correlationAttributeList, payloadAttributeList);
        } catch (MalformedStreamDefinitionException e) {
            throw new SiddhiAppValidationException(e.getMessage(), e);
        }
        if (0 < metaDataMap.size()) {
            attributePositionMap.put(WSO2EventMapperUtils.InputDataType.META_DATA, metaDataMap);
        }
        if (0 < correlationDataMap.size()) {
            attributePositionMap.put(WSO2EventMapperUtils.InputDataType.CORRELATION_DATA, correlationDataMap);
        }
        if (0 < payloadDataMap.size()) {
            attributePositionMap.put(WSO2EventMapperUtils.InputDataType.PAYLOAD_DATA, payloadDataMap);
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
            Object outputAttributes[] = new Object[attributeList.size()];
            Map<Integer, Integer> metaPositions = attributePositionMap.get(WSO2EventMapperUtils.
                    InputDataType.META_DATA);
            Map<Integer, Integer> correlationPositions = attributePositionMap.get(
                    WSO2EventMapperUtils.InputDataType.CORRELATION_DATA);
            Map<Integer, Integer> payloadPositions = attributePositionMap.get(WSO2EventMapperUtils.
                    InputDataType.PAYLOAD_DATA);
            if (null != metaPositions) {
                for (Map.Entry<Integer, Integer> entry : metaPositions.entrySet()) {
                    outputAttributes[entry.getValue()] = wso2event.getMetaData()[entry.getKey()];
                }
            }
            if (null != correlationPositions) {
                for (Map.Entry<Integer, Integer> entry : correlationPositions.entrySet()) {
                    outputAttributes[entry.getValue()] = wso2event.getCorrelationData()[entry.getKey()];
                }
            }
            if (null != payloadPositions) {
                for (Map.Entry<Integer, Integer> entry : payloadPositions.entrySet()) {
                    outputAttributes[entry.getValue()] = wso2event.getPayloadData()[entry.getKey()];
                }
            }
            if (-1 != arbitraryAttributeIndex) {
                outputAttributes[arbitraryAttributeIndex] = wso2event.getArbitraryDataMap();
            }
            inputEventHandler.sendEvent(new Event(wso2event.getTimeStamp(), outputAttributes));
        } else {
            LOGGER.warn("Event object is invalid. Expected WSO2Event, but found " + eventObject.getClass()
                    .getCanonicalName() + ". Hence dropping the event");
        }
    }

    public org.wso2.carbon.databridge.commons.StreamDefinition getWSO2StreamDefinition() {
        return this.streamDefinition;
    }
}
