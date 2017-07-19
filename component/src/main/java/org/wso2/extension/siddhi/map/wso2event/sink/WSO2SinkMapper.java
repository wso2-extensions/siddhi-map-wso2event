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
package org.wso2.extension.siddhi.map.wso2event.sink;

import org.wso2.extension.siddhi.map.wso2event.WSO2EventMapperUtils;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.output.sink.SinkListener;
import org.wso2.siddhi.core.stream.output.sink.SinkMapper;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.core.util.transport.TemplateBuilder;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.wso2.extension.siddhi.map.wso2event.WSO2EventMapperUtils.ARBITRARY_DATA_PREFIX;
import static org.wso2.extension.siddhi.map.wso2event.WSO2EventMapperUtils.CORRELATION_DATA_PREFIX;
import static org.wso2.extension.siddhi.map.wso2event.WSO2EventMapperUtils.CUSTOM_MAPPING_ENABLED;
import static org.wso2.extension.siddhi.map.wso2event.WSO2EventMapperUtils.META_DATA_PREFIX;


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
                "format which adheres to the defined stream.\n" +
                "Following prefixes will be used to identify different attributes such as meta, correlation, " +
                "payload and arbitrary. Prefixes available," +
                "1. meta_ - metaData," +
                "2. correlation_ - correlationData," +
                "3. arbitrary_ - value contained in the arbitraryMap mapped to the key defined after the prefix" +
                "If the above prefixes are not used, the attribute is taken as payload data.",
        parameters = {
                @Parameter(name = "enable.custom.mapping",
                        description = "Specifies if the custom mapping should be enabled. If enabled, the attributes " +
                                "defined in the stream definition must be given as parameters mapping to the " +
                                "attribute name in the wso2event.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false")
        },
        examples = {
                @Example(
                        syntax = "@sink(type='wso2event', @map(type='wso2event')); " +
                                "define stream FooStream (symbol string, price float, volume long);",
                        description = "Above configuration will perform a WSO2 default input mapping which will " +
                                "generate the following output.\n" +
                                "Wso2event = {\n" +
                                "                 streamId: barStream:1.0.0,\n" +
                                "                 timestamp: FooStream_siddhi_event_timestamp,\n" +
                                "                 metaData: [],\n" +
                                "                 correlationData: [],\n" +
                                "                 payloadData: [symbol, price, volume]\n" +
                                "            }\n"),
                @Example(
                        syntax = "@sink(type='wso2event', @map(type='wso2event', enable.custom.mapping='true'," +
                                "timestamp='meta_timestamp', symbol='symbol', price='price', volume='volume', " +
                                "portfolioID='arbitrary_portfolio_ID')) " +
                                "define stream FooStream (timestamp long, symbol string, price float, " +
                                "volume long, portfolioID string);",
                        description = "Above configuration will perform a WSO2 custom mapping which will produce the " +
                                "following output.\n" +
                                "Wso2event = {\n" +
                                "                 streamId: barStream:1.0.0,\n" +
                                "                 timeStamp: FooStream_siddhi_event_timestamp,\n" +
                                "                 metaData: [meta_timestamp],\n" +
                                "                 correlationData: [],\n" +
                                "                 payloadData: [symbol, price, volume],\n" +
                                "                 arbitraryDataMap: arbitrary\n" +
                                "            }\n" +
                                "where arbitrary object contains a map entry : <portfolio_ID, 'value for portfolioID'>")
        }
)
public class WSO2SinkMapper extends SinkMapper {

    private String outputStreamId;

    private Map<Integer, Integer> metaDataMap;
    private Map<Integer, Integer> correlationDataMap;
    private Map<Integer, Integer> payloadDataMap;
    private Map<String, Integer> arbitraryDataMap;

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{org.wso2.carbon.databridge.commons.Event.class,
                org.wso2.carbon.databridge.commons.Event[].class};
    }

    /**
     * Initialize the mapper and the mapping configurations.
     *
     * @param streamDefinition The stream definition
     * @param optionHolder     Option holder containing static and dynamic options
     * @param templateBuilder  Unmapped payload for reference
     * @param configReader     Config
     * @param siddhiAppContext SiddhiApp context
     */
    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, TemplateBuilder templateBuilder,
                     ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

        List<Attribute> attributeList = streamDefinition.getAttributeList();

        this.metaDataMap = new HashMap<>();
        this.correlationDataMap = new HashMap<>();
        this.payloadDataMap = new HashMap<>();
        this.arbitraryDataMap = new HashMap<>();
        List<String> customMappedAttributes = new ArrayList<>();

        boolean customMappingEnabled = Boolean.parseBoolean(
                optionHolder.validateAndGetStaticValue(CUSTOM_MAPPING_ENABLED, "false"));
        if (customMappingEnabled) {
            customMappedAttributes = attributeList.stream()
                    .map((attribute) -> optionHolder.validateAndGetStaticValue(attribute.getName()))
                    .collect(Collectors.toList());
        }

        int metaCount = 0, correlationCount = 0, payloadCount = 0;

        for (int i = 0; i < attributeList.size(); i++) {
            String attributeName;
            if (customMappingEnabled) {
                attributeName = customMappedAttributes.get(i);
            } else {
                attributeName = attributeList.get(i).getName();
            }
            Attribute.Type attributeType = attributeList.get(i).getType();

            if (attributeName.startsWith(META_DATA_PREFIX)) {
                //i'th location value of the export stream will be copied to meta array's metaCount'th location
                this.metaDataMap.put(metaCount, i);
                metaCount++;
            } else if (attributeName.startsWith(CORRELATION_DATA_PREFIX)) {
                this.correlationDataMap.put(correlationCount, i);
                correlationCount++;
            } else if (attributeName.startsWith(ARBITRARY_DATA_PREFIX)) {
                if (attributeType.equals(Attribute.Type.STRING)) {
                    this.arbitraryDataMap.put(attributeName.replace(ARBITRARY_DATA_PREFIX, ""), i);
                } else {
                    throw new SiddhiAppCreationException("Arbitrary map value has been mapped to '"
                            + attributeType + "' in Siddhi app '" + siddhiAppContext.getName() + "'. " +
                            "However, arbitrary map value can only be mapped to type 'String'.");
                }
            } else {
                this.payloadDataMap.put(payloadCount, i);
                payloadCount++;
            }
        }

        this.outputStreamId = streamDefinition.getId() + WSO2EventMapperUtils.STREAM_NAME_VER_DELIMITER +
                WSO2EventMapperUtils.DEFAULT_STREAM_VERSION;
    }

    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder, TemplateBuilder templateBuilder,
                           SinkListener sinkListener) {
        sinkListener.publish(performMapping(event));
    }

    /**
     * Map and publish the given {@link Event} array.
     *
     * @param events          Event object array
     * @param optionHolder    option holder containing static and dynamic options
     * @param templateBuilder Unmapped payload for reference
     * @param sinkListener    output transport callback
     */
    @Override
    public void mapAndSend(Event[] events, OptionHolder optionHolder, TemplateBuilder templateBuilder,
                           SinkListener sinkListener) {
        for (Event event : events) {
            sinkListener.publish(performMapping(event));
        }
    }

    /**
     * Convert the given {@link Event} to WSO2 {@link org.wso2.carbon.databridge.commons.Event}.
     *
     * @param event Event object
     * @return the constructed WSO2 Event
     */
    private org.wso2.carbon.databridge.commons.Event performMapping(Event event) {
        org.wso2.carbon.databridge.commons.Event wso2event = new org.wso2.carbon.databridge.commons.Event();
        wso2event.setTimeStamp(event.getTimestamp());
        wso2event.setStreamId(this.outputStreamId);
        Object[] eventData = event.getData();
        if (eventData.length > 0) {

            Object[] metaArray = new Object[this.metaDataMap.size()];
            for (Map.Entry<Integer, Integer> entry : this.metaDataMap.entrySet()) {
                metaArray[entry.getKey()] = eventData[entry.getValue()];
            }
            wso2event.setMetaData(metaArray);

            Object[] correlationArray = new Object[this.correlationDataMap.size()];
            for (Map.Entry<Integer, Integer> entry : this.correlationDataMap.entrySet()) {
                correlationArray[entry.getKey()] = eventData[entry.getValue()];
            }
            wso2event.setCorrelationData(correlationArray);

            Object[] payloadArray = new Object[this.payloadDataMap.size()];
            for (Map.Entry<Integer, Integer> entry : this.payloadDataMap.entrySet()) {
                payloadArray[entry.getKey()] = eventData[entry.getValue()];
            }
            wso2event.setPayloadData(payloadArray);

            Map<String, String> arbitraryDataMap = new HashMap<>();
            for (Map.Entry<String, Integer> entry : this.arbitraryDataMap.entrySet()) {
                arbitraryDataMap.put(entry.getKey(), (String) eventData[entry.getValue()]);
            }
            wso2event.setArbitraryDataMap(arbitraryDataMap);
        }
        return wso2event;
    }

}
