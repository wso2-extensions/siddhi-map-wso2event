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
package io.sidhi.extension.map.wso2event.sink;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.output.sink.SinkListener;
import io.siddhi.core.stream.output.sink.SinkMapper;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.core.util.transport.TemplateBuilder;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;

import io.sidhi.extension.map.wso2event.util.AttributePosition;
import io.sidhi.extension.map.wso2event.util.WSO2EventMapperUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Mapper class to convert a Siddhi message to a WSO2 event. We will be using the stream definition and populate the
 * WSO2 event attribute values accordingly and construct the WSO2 event. In case of null values, Same will be added
 * to the WSO2 event attributes as well.
 */
@Extension(

        name = "wso2event",
        namespace = "sinkMapper",
        description = "This section explains how to map events processed via Siddhi in order to publish them in " +
                "the `WSO2Event` format. In default mapping, the format used is the pre-defined WSO2Event format " +
                "that adheres to the schema of the defined stream. In order to use custom mapping, additional " +
                "parameters need to be configured (i.e., in addition to the format type).\n",
        examples = {
                @Example(
                        syntax = "`@sink(type='wso2event', @map(type='wso2event')); `" +
                                "`define stream FooStream (symbol string, price float, volume long);`",
                        description = "This configuration performs a WSO2 default input mapping that generates the" +
                                "following output.\n" +
                                "Wso2event = {\n" +
                                "                 streamId: barStream:1.0.0,\n" +
                                "                 timestamp: FooStream_siddhi_event_timestamp,\n" +
                                "                 metaData: [],\n" +
                                "                 correlationData: [],\n" +
                                "                 payloadData: [symbol, price, volume]\n" +
                                "            }\n"),

                @Example(
                        syntax = "@sink(type='wso2event', " +
                                "@map(type='wso2event'," +
                                "meta_timestamp='timestamp', " +
                                "symbol='symbol', " +
                                "price='price'," +
                                "volume='volume', " +
                                "arbitrary_portfolioID='portfolio_ID')) " +
                                "define stream FooStream (timestamp long, symbol string, price float, " +
                                "volume long, portfolioID string);",
                        description = "This configuration performs a custom mapping and produces the following " +
                                "output.\n" +
                                "Wso2event = {\n" +
                                "                 streamId: barStream:1.0.0,\n" +
                                "                 timeStamp: FooStream_siddhi_event_timestamp,\n" +
                                "                 metaData: [meta_timestamp],\n" +
                                "                 correlationData: [],\n" +
                                "                 payloadData: [symbol, price, volume],\n" +
                                "                 arbitraryDataMap: arbitrary\n" +
                                "            }\n" +
                                "The value of the `arbitrary_portfolioID` attribute in the processed Siddhi event " +
                                "is assigned as the value for the `portfolio_ID` attribute in the published " +
                                "`WSO2Event` event.'>")
        }
)
public class WSO2SinkMapper extends SinkMapper {

    private String outputStreamId;
    private int numMetaAttributes;
    private int numCorrelationAttributes;
    private int numPayloadAttributes;
    private Integer[] metaDataPositions;
    private Integer[] correlationDataPositions;
    private Integer[] payloadDataPositions;
    private AttributePosition[] arbitraryDataPositions;

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
     * @param streamDefinition          The stream definition
     * @param optionHolder              Option holder containing static and dynamic options
     * @param payloadTemplateBuilderMap Unmapped payload for reference
     * @param configReader              Config
     * @param siddhiAppContext          SiddhiApp context
     */
    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                     Map<String, TemplateBuilder> payloadTemplateBuilderMap, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        List<Attribute> attributeList = streamDefinition.getAttributeList();

        Map<String, String> mappedAttributes = new HashMap<>();
        Set<String> staticOptionsKeys = optionHolder.getStaticOptionsKeys();
        staticOptionsKeys.remove("type");
        boolean customMappingEnabled = staticOptionsKeys.size() > 0;
        if (customMappingEnabled) {
            staticOptionsKeys.forEach((key) -> mappedAttributes.put(optionHolder.validateAndGetStaticValue(key), key));
        }

        List<Integer> metaDataList = new ArrayList<>();
        List<Integer> correlationDataList = new ArrayList<>();
        List<Integer> payloadDataList = new ArrayList<>();
        List<AttributePosition> arbitraryDataList = new ArrayList<>();

        for (int i = 0; i < attributeList.size(); i++) {
            String attributeName = attributeList.get(i).getName();
            if (customMappingEnabled) {
                if (mappedAttributes.get(attributeName) != null) {
                    attributeName = mappedAttributes.get(attributeName);
                } else {
                    throw new SiddhiAppCreationException("The siddhi attribute '" + attributeName + "', is not " +
                            "mapped in the annotation @map() for custom mapping.");
                }
            }
            Attribute.Type attributeType = attributeList.get(i).getType();

            if (attributeName.startsWith(WSO2EventMapperUtils.META_DATA_PREFIX)) {
                //i'th location value of the export stream will be copied to meta array's metaCount'th location
                metaDataList.add(i);
            } else if (attributeName.startsWith(WSO2EventMapperUtils.CORRELATION_DATA_PREFIX)) {
                correlationDataList.add(i);
            } else if (attributeName.startsWith(WSO2EventMapperUtils.ARBITRARY_DATA_PREFIX)) {
                if (attributeType.equals(Attribute.Type.STRING)) {
                    arbitraryDataList.add(
                            new AttributePosition(attributeName.replace(WSO2EventMapperUtils.ARBITRARY_DATA_PREFIX,
                                    ""), i));
                } else {
                    throw new SiddhiAppCreationException("Arbitrary map value has been mapped to '"
                            + attributeType + "' in Siddhi app '" + siddhiAppContext.getName() + "'. " +
                            "However, arbitrary map value can only be mapped to type 'String'.");
                }
            } else {
                payloadDataList.add(i);
            }
        }

        this.numMetaAttributes = metaDataList.size();
        this.numCorrelationAttributes = correlationDataList.size();
        this.numPayloadAttributes = payloadDataList.size();
        this.metaDataPositions = metaDataList.toArray(new Integer[this.numMetaAttributes]);
        this.correlationDataPositions = correlationDataList.toArray(new Integer[this.numCorrelationAttributes]);
        this.payloadDataPositions = payloadDataList.toArray(new Integer[this.numPayloadAttributes]);
        this.arbitraryDataPositions = arbitraryDataList.toArray(new AttributePosition[arbitraryDataList.size()]);
        this.outputStreamId = streamDefinition.getId() + WSO2EventMapperUtils.STREAM_NAME_VER_DELIMITER +
                WSO2EventMapperUtils.DEFAULT_STREAM_VERSION;
    }

    /**
     * Map and publish the given {@link Event} array.
     *
     * @param event                     Event object
     * @param optionHolder              option holder containing static and dynamic options
     * @param payloadTemplateBuilderMap Unmapped payload for reference
     * @param sinkListener              output transport callback
     */
    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder,
                           Map<String, TemplateBuilder> payloadTemplateBuilderMap, SinkListener sinkListener) {
        sinkListener.publish(performMapping(event));
    }

    /**
     * Map and publish the given {@link Event} array.
     *
     * @param events                    Event object array
     * @param optionHolder              option holder containing static and dynamic options
     * @param payloadTemplateBuilderMap Unmapped payload for reference
     * @param sinkListener              output transport callback
     */
    @Override
    public void mapAndSend(Event[] events, OptionHolder optionHolder,
                           Map<String, TemplateBuilder> payloadTemplateBuilderMap, SinkListener sinkListener) {
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

            Object[] metaArray = new Object[this.numMetaAttributes];
            for (int i = 0; i < this.numMetaAttributes; i++) {
                metaArray[i] = eventData[this.metaDataPositions[i]];
            }
            wso2event.setMetaData(metaArray);

            Object[] correlationArray = new Object[this.numCorrelationAttributes];
            for (int i = 0; i < this.numCorrelationAttributes; i++) {
                correlationArray[i] = eventData[this.correlationDataPositions[i]];
            }
            wso2event.setCorrelationData(correlationArray);

            Object[] payloadArray = new Object[this.numPayloadAttributes];
            for (int i = 0; i < this.numPayloadAttributes; i++) {
                payloadArray[i] = eventData[this.payloadDataPositions[i]];
            }
            wso2event.setPayloadData(payloadArray);

            Map<String, String> arbitraryDataMap = new HashMap<>();
            for (AttributePosition arbitraryDataPosition : this.arbitraryDataPositions) {
                arbitraryDataMap.put(
                        arbitraryDataPosition.getAttributeName(),
                        (String) eventData[arbitraryDataPosition.getAttributePosition()]
                );
            }
            wso2event.setArbitraryDataMap(arbitraryDataMap);
        }
        return wso2event;
    }

}
