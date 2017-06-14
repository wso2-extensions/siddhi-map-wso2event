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
import org.wso2.carbon.databridge.commons.AttributeType;
import org.wso2.carbon.databridge.commons.exception.MalformedStreamDefinitionException;
import org.wso2.extension.siddhi.map.wso2event.service.StreamDefinitionHolder;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.output.sink.SinkListener;
import org.wso2.siddhi.core.stream.output.sink.SinkMapper;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.core.util.transport.TemplateBuilder;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/**
 * Mapper class to convert a Siddhi message to a WSO2 event. We will be using the stream definition and populate the
 * WSO2 event attribute values accordingly and construct the WSO2 event. In case of null values, Same will be added
 * to the WSO2 event attributes as well.
 */
@Extension(
        name = "wso2event",
        namespace = "sinkMapper",
        description = "Event to WSO2 output mapper. Transports which publish WSO2 messages, can utilize this " +
                "extension to convert the Siddhi event to WSO2 event objects. Users can send pre-defined WSO2 event " +
                "format which adheres from the defined stream.",
        parameters = {
                @Parameter(name = "wso2event.stream.id",
                        description =
                                "Used to provide the outgoing event's wso2event stream id. This should only contain " +
                                        "simple letters and should not be starting with numbers. " +
                                        "eg: wso2.stream.id, wso2.st3eam.id is valid while, 2wso.stream.id is invalid",
                        type = {DataType.STRING}),
                @Parameter(name = "arbitrary.map",
                        description =
                                "Used to provide the attribute name of the stream which the arbitrary object to be " +
                                        "mapped from" +
                                        "eg: arbitrary.map='foo' foo is a attribute name in the stream definition " +
                                        "with the attribute type object",
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "@sink(type='wso2event', @map(type='wso2event', " +
                                "wso2event.stream.id='barStream:1.0.0')); " +
                                "define stream FooStream (symbol string, price float, volume long);",
                        description = "Above configuration will do a WSO2 input mapping which will generate below " +
                                "output" +
                                "Wso2event = {" +
                                "                 streamId: barStream:1.0.0,\n" +
                                "                 timeStamp: FooStream_siddhi_event_timestamp,\n" +
                                "                 metaData: [],\n" +
                                "                 correlationData: [],\n" +
                                "                 payloadData: [symbol, price, volume]\n" +
                                "            }"),
                @Example(
                        syntax = "@sink(type='wso2event', @map(type='wso2event',  " +
                                "wso2event.stream.id='barStream:1.0.0', " +
                                "arbitrary.map='arbitrary_object')) " +
                                "define stream FooStream (meta_timestamp long, symbol string, price float, " +
                                "volume long, arbitrary_object object);",
                        description = "Above configuration will perform a WSO2 mapping with the arbitrary object " +
                                "which will produce below output WSO2 event message" +
                                "Wso2event = {" +
                                "                 streamId: barStream:1.0.0,\n" +
                                "                 timeStamp: FooStream_siddhi_event_timestamp,\n" +
                                "                 metaData: [meta_timestamp],\n" +
                                "                 correlationData: [],\n" +
                                "                 payloadData: [symbol, price, volume],\n" +
                                "                 arbitraryDataMap: arbitrary\n" +
                                "            }")
        }
)
public class WSO2SinkMapper extends SinkMapper {
    private static final Logger LOG = Logger.getLogger(WSO2SinkMapper.class);
    private String outputStreamId;
    private static final String META_DATA_PREFIX = "meta_";
    private static final String CORRELATION_DATA_PREFIX = "correlation_";
    private static final String TO_STREAM_ID = "wso2event.stream.id";
    private static final String ARBITRARY_MAP_ATTRIBUTE = "arbitrary.map";
    private static final String STREAM_NAME_VER_DELIMITER = ":";
    private int arbitraryAttributeIndex = -1;
    private Map<InputDataType, Map<Integer, Integer>> attributePositionMap = new HashMap<>();

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }

    /**
     * Initialize the mapper and the mapping configurations.
     *
     * @param streamDefinition       The stream definition
     * @param optionHolder           Option holder containing static and dynamic options
     * @param payloadTemplateBuilder Unmapped payload for reference
     * @param mapperConfigReader
     */
    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                     TemplateBuilder payloadTemplateBuilder, ConfigReader mapperConfigReader) {
        if (payloadTemplateBuilder != null) {   //custom mapping
            throw new ExecutionPlanValidationException("WSO2 Transport does not support custom mapping. Please remove" +
                    " @attributes section in mapping.");
        }
        this.outputStreamId = optionHolder.validateAndGetStaticValue(TO_STREAM_ID,
                streamDefinition.getId() + STREAM_NAME_VER_DELIMITER + "1.0.0");
        String arbitraryAttributeName = optionHolder.validateAndGetStaticValue(ARBITRARY_MAP_ATTRIBUTE, null);
        List<Attribute> attributeList = streamDefinition.getAttributeList();
        Map<Integer, Integer> payloadDataMap = new TreeMap<Integer, Integer>();
        Map<Integer, Integer> metaDataMap = new TreeMap<Integer, Integer>();
        Map<Integer, Integer> correlationDataMap = new TreeMap<Integer, Integer>();
        int metaCount = 0, correlationCount = 0, payloadCount = 0;
        List<org.wso2.carbon.databridge.commons.Attribute> metaAttributeList = new ArrayList<>();
        List<org.wso2.carbon.databridge.commons.Attribute> correlationAttributeList = new ArrayList<>();
        List<org.wso2.carbon.databridge.commons.Attribute> payloadAttributeList = new ArrayList<>();
        org.wso2.carbon.databridge.commons.Attribute wso2eventAttribute;
        for (int i = 0; i < attributeList.size(); i++) {
            if (attributeList.get(i).getName().startsWith(META_DATA_PREFIX)) {
                //i'th location value of the export stream will be copied to meta array's metaCount'th location
                wso2eventAttribute = createWso2EventAttribute(attributeList.get(i));
                if (null != wso2eventAttribute) {
                    metaAttributeList.add(wso2eventAttribute);
                }
                metaDataMap.put(metaCount, i);
                metaCount++;
            } else if (attributeList.get(i).getName().startsWith(CORRELATION_DATA_PREFIX)) {
                wso2eventAttribute = createWso2EventAttribute(attributeList.get(i));
                if (null != wso2eventAttribute) {
                    correlationAttributeList.add(wso2eventAttribute);
                }
                correlationDataMap.put(correlationCount, i);
                correlationCount++;
            } else if (null != arbitraryAttributeName &&
                    attributeList.get(i).getName().equals(arbitraryAttributeName)) {
                if (Type.OBJECT.equals(attributeList.get(i).getType())) {
                    arbitraryAttributeIndex = i;
                } else {
                    throw new ExecutionPlanValidationException("defined arbitrary.map attribute in the " +
                            "mapping is type: " + attributeList.get(i).getType() + ". It should be type: " +
                            Type.OBJECT);
                }
            } else {
                wso2eventAttribute = createWso2EventAttribute(attributeList.get(i));
                if (null != wso2eventAttribute) {
                    payloadAttributeList.add(wso2eventAttribute);
                }
                payloadDataMap.put(payloadCount, i);
                payloadCount++;
            }
        }

        String streamNameVersion[] = outputStreamId.split(STREAM_NAME_VER_DELIMITER);
        try {
            org.wso2.carbon.databridge.commons.StreamDefinition wso2StreamDefinition = new org.wso2.carbon.databridge
                    .commons.StreamDefinition(streamNameVersion[0], streamNameVersion[1]);
            wso2StreamDefinition.setMetaData(metaAttributeList);
            wso2StreamDefinition.setCorrelationData(correlationAttributeList);
            wso2StreamDefinition.setPayloadData(payloadAttributeList);
            StreamDefinitionHolder.setStreamDefinition(wso2StreamDefinition);
        } catch (MalformedStreamDefinitionException e) {
            throw new ExecutionPlanValidationException(e.getMessage(), e);
        }
        if (0 < metaDataMap.size()) {
            attributePositionMap.put(InputDataType.META_DATA, metaDataMap);
        }
        if (0 < correlationDataMap.size()) {
            attributePositionMap.put(InputDataType.CORRELATION_DATA, correlationDataMap);
        }
        if (0 < payloadDataMap.size()) {
            attributePositionMap.put(InputDataType.PAYLOAD_DATA, payloadDataMap);
        }
    }

    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder, TemplateBuilder payloadTemplateBuilder,
                           SinkListener sinkListener, DynamicOptions dynamicOptions)
            throws ConnectionUnavailableException {
        sinkListener.publish(constructDefaultMapping(event), dynamicOptions);
    }

    /**
     * Map and publish the given {@link Event} array
     *
     * @param events                 Event object array
     * @param optionHolder           option holder containing static and dynamic options
     * @param payloadTemplateBuilder Unmapped payload for reference
     * @param sinkListener           output transport callback
     */
    @Override
    public void mapAndSend(Event[] events, OptionHolder optionHolder, TemplateBuilder payloadTemplateBuilder,
                           SinkListener sinkListener, DynamicOptions dynamicOptions)
            throws ConnectionUnavailableException {
        if (events.length < 1) {        //todo valid case?
            return;
        }
        for (Event event : events) {
            sinkListener.publish(constructDefaultMapping(event), dynamicOptions);
        }
    }

    /**
     * Convert the given {@link Event} to WSO2 {@link org.wso2.carbon.databridge.commons.Event}
     *
     * @param event Event object
     * @return the constructed WSO2 Event
     */
    private org.wso2.carbon.databridge.commons.Event constructDefaultMapping(Event event) {
        org.wso2.carbon.databridge.commons.Event wso2event = new org.wso2.carbon.databridge.commons.Event();
        wso2event.setTimeStamp(event.getTimestamp());
        wso2event.setStreamId(outputStreamId);
        Object[] eventData = event.getData();
        if (eventData.length > 0) {
            Map<Integer, Integer> metaPositions = attributePositionMap.get(InputDataType.META_DATA);
            Map<Integer, Integer> correlationPositions = attributePositionMap.get(InputDataType.CORRELATION_DATA);
            Map<Integer, Integer> payloadPositions = attributePositionMap.get(InputDataType.PAYLOAD_DATA);
            if (null != metaPositions) {
                Object[] metaArray = new Object[metaPositions.size()];
                for (Map.Entry<Integer, Integer> entry : metaPositions.entrySet()) {
                    metaArray[entry.getKey()] = eventData[entry.getValue()];
                }
                wso2event.setMetaData(metaArray);
            }
            if (null != correlationPositions) {
                Object[] correlationArray = new Object[correlationPositions.size()];
                for (Map.Entry<Integer, Integer> entry : correlationPositions.entrySet()) {
                    correlationArray[entry.getKey()] = eventData[entry.getValue()];
                }
                wso2event.setCorrelationData(correlationArray);
            }
            if (null != payloadPositions) {
                Object[] payloadArray = new Object[payloadPositions.size()];
                for (Map.Entry<Integer, Integer> entry : payloadPositions.entrySet()) {
                    payloadArray[entry.getKey()] = eventData[entry.getValue()];
                }
                wso2event.setPayloadData(payloadArray);
            }
            if (-1 != arbitraryAttributeIndex) {
                //null value will be assigned if there is no map.
                //todo check whether the map is Map<String, String> ??
                wso2event.setArbitraryDataMap((Map<String, String>) eventData[arbitraryAttributeIndex]);
            }
        }
        return wso2event;
    }

    /**
     * Convert the given {@link Attribute} to WSO2 {@link org.wso2.carbon.databridge.commons.Attribute}
     *
     * @param attribute Siddhi Event attribute object
     * @return the created WSO2 Event attribute
     */
    private org.wso2.carbon.databridge.commons.Attribute createWso2EventAttribute(Attribute attribute) {
        org.wso2.carbon.databridge.commons.AttributeType attribute1;
        switch (attribute.getType()) {
            case BOOL:
                attribute1 = AttributeType.BOOL;
                break;
            case STRING:
                attribute1 = AttributeType.STRING;
                break;
            case INT:
                attribute1 = AttributeType.INT;
                break;
            case LONG:
                attribute1 = AttributeType.LONG;
                break;
            case FLOAT:
                attribute1 = AttributeType.FLOAT;
                break;
            case DOUBLE:
                attribute1 = AttributeType.DOUBLE;
                break;
            default:
                attribute1 = null;
        }
        if (null != attribute1) {
            return new org.wso2.carbon.databridge.commons.Attribute(attribute.getName(), attribute1);
        } else {
            return null;
        }
    }

    private enum InputDataType {
        META_DATA, CORRELATION_DATA, PAYLOAD_DATA
    }
}
