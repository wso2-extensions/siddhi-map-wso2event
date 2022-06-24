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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.source.AttributeMapping;
import io.siddhi.core.stream.input.source.InputEventHandler;
import io.siddhi.core.stream.input.source.SourceMapper;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.carbon.databridge.commons.exception.MalformedStreamDefinitionException;
import org.wso2.extension.siddhi.map.wso2event.util.AttributePosition;
import org.wso2.extension.siddhi.map.wso2event.util.WSO2EventMapperUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * This mapper converts WSO2 input event to {@link io.siddhi.core.event.ComplexEventChunk}. This extension
 * accepts WSO2 events from WSO2 receiver and maps attribute values to the defined stream.
 */
@Extension(
        name = "wso2event",
        namespace = "sourceMapper",
        description = "This extention is a WSO2Event input mapper type that converts events received in `WSO2Event`" +
                " format to Siddhi events before they are processed. In default mapping, the format used is" +
                " the pre-defined WSO2Event format that adheres to the schema of the defined stream. In order to use " +
                "custom mapping, additional parameters need to be configured (i.e., in addition to the format type)." +
                " This is useful when the events are generated by a third party and as a result, you do not have" +
                " control over how they are formatted.\n",

        examples = {
                @Example(
                        syntax = "@source(type=’wso2event’, @map(type=’wso2event’) " +
                                "define stream FooStream (meta_timestamp long, symbol string, price float, " +
                                "volume long);",
                        description = "This query performs a default mapping to convert an event of the `WSO2Event` " +
                                "format to a Siddhi event. The expected input is as follows.\n" +
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
                                "define stream FooStream (timestamp long, symbol string, price float, " +
                                "volume long, portfolioId string)); ",
                        description = "This query performs a WSO2 mapping. Expected input is as follows." +
                                "follows,\n" +
                                "Wso2event = {\n" +
                                "                streamId: org.wso2event.fooStream:1.0.0,\n" +
                                "                timeStamp: 431434134134,\n" +
                                "                metaData: [timestamp],\n" +
                                "                correlationData: [],\n" +
                                "                payloadData: [symbol, price, volume],\n" +
                                "                arbitraryDataPosition: objectMap,\n" +
                                "            }\n" +
                                "In this query, the WSO2 mapper gets the value of the `arbitrary_portfolio_id` " +
                                "attribute in the `WSO2Event` event received, and assigns it to the `portfolioId` " +
                                "attribute in the corresponding Siddhi event."),
        }
)
public class WSO2SourceMapper extends SourceMapper {
    private static final Logger LOGGER = LogManager.getLogger(WSO2SourceMapper.class);

    private org.wso2.carbon.databridge.commons.StreamDefinition streamDefinition;
    private int numAttributes;
    private int numMetaAttributes;
    private int numCorrelationAttributes;
    private int numPayloadAttributes;
    private Integer[] metaDataPositions;
    private Integer[] correlationDataPosition;
    private Integer[] payloadDataPosition;
    private AttributePosition[] arbitraryDataPosition;

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

        List<org.wso2.carbon.databridge.commons.Attribute> metaAttributeList = new ArrayList<>();
        List<org.wso2.carbon.databridge.commons.Attribute> correlationAttributeList = new ArrayList<>();
        List<org.wso2.carbon.databridge.commons.Attribute> payloadAttributeList = new ArrayList<>();

        List<Integer> metaDataPositions = new ArrayList<>();
        List<Integer> correlationDataPositions = new ArrayList<>();
        List<Integer> payloadDataPositions = new ArrayList<>();
        List<AttributePosition> arbitraryDataPositions = new ArrayList<>();

        for (int i = 0; i < attributeList.size(); i++) {
            String attributeName;
            if (list != null && list.size() > 0) {
                attributeName = list.get(i).getMapping();
            } else {
                attributeName = attributeList.get(i).getName();
            }
            Attribute.Type attributeType = attributeList.get(i).getType();

            if (attributeName.startsWith(WSO2EventMapperUtils.META_DATA_PREFIX)) {
                //meta array's metaCount'th attribute of import stream will be mapped to the i'th
                // location of the export stream.
                metaAttributeList.add(WSO2EventMapperUtils.createWso2EventAttribute(attributeName, attributeType));
                metaDataPositions.add(i);
            } else if (attributeName.startsWith(WSO2EventMapperUtils.CORRELATION_DATA_PREFIX)) {
                correlationAttributeList.add(WSO2EventMapperUtils
                        .createWso2EventAttribute(attributeName, attributeType));
                correlationDataPositions.add(i);
            } else if (attributeName.startsWith(WSO2EventMapperUtils.ARBITRARY_DATA_PREFIX)) {
                if (attributeType.equals(Attribute.Type.STRING)) {
                    arbitraryDataPositions.add(
                            new AttributePosition(attributeName.replace(WSO2EventMapperUtils.ARBITRARY_DATA_PREFIX,
                                    ""), i));
                } else {
                    throw new SiddhiAppCreationException("Arbitrary Map attribute '" + attributeName + "' has " +
                            "been mapped to '" + attributeType + "' in Siddhi app '" + siddhiAppContext.getName() +
                            "'. However, arbitrary map value can only be mapped to type 'String'.");
                }
            } else {
                payloadAttributeList.add(WSO2EventMapperUtils.createWso2EventAttribute(attributeName, attributeType));
                payloadDataPositions.add(i);
            }
        }


        this.numMetaAttributes = metaDataPositions.size();
        this.numCorrelationAttributes = correlationDataPositions.size();
        this.numPayloadAttributes = payloadDataPositions.size();
        this.metaDataPositions = metaDataPositions.toArray(new Integer[this.numMetaAttributes]);
        this.correlationDataPosition = correlationDataPositions.toArray(new Integer[this.numCorrelationAttributes]);
        this.payloadDataPosition = payloadDataPositions.toArray(new Integer[this.numPayloadAttributes]);
        this.arbitraryDataPosition = arbitraryDataPositions.toArray(
                new AttributePosition[arbitraryDataPositions.size()]);

        try {
            this.streamDefinition = WSO2EventMapperUtils.createWSO2EventStreamDefinition(streamDefinition.getId(),
                    metaAttributeList, correlationAttributeList, payloadAttributeList);
        } catch (MalformedStreamDefinitionException e) {
            throw new SiddhiAppValidationException(e.getMessage(), e);
        }
    }

    /**
     * Receives an event as an WSO2 event from WSO2 Receiver {@link io.siddhi.core.stream.input.source.Source},
     * converts it to a {@link io.siddhi.core.event.ComplexEventChunk} and send to the
     * {@link io.siddhi.core.query.output.callback.OutputCallback}.
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

            for (int i = 0; i < this.numMetaAttributes; i++) {
                outputAttributes[this.metaDataPositions[i]] = wso2event.getMetaData()[i];
            }

            for (int i = 0; i < this.numCorrelationAttributes; i++) {
                outputAttributes[this.correlationDataPosition[i]] =
                        wso2event.getCorrelationData()[i];
            }

            for (int i = 0; i < this.numPayloadAttributes; i++) {
                outputAttributes[this.payloadDataPosition[i]] = wso2event.getPayloadData()[i];
            }

            for (AttributePosition arbitraryDataPosirion : this.arbitraryDataPosition) {
                outputAttributes[arbitraryDataPosirion.getAttributePosition()] =
                        wso2event.getArbitraryDataMap().get(arbitraryDataPosirion.getAttributeName());
            }

            for (AttributePosition anArbitraryDataList : this.arbitraryDataPosition) {
                outputAttributes[anArbitraryDataList.getAttributePosition()] =
                        wso2event.getArbitraryDataMap().get(anArbitraryDataList.getAttributeName());
            }

            inputEventHandler.sendEvent(new Event(wso2event.getTimeStamp(), outputAttributes));
        } else {
            LOGGER.warn("Event object is invalid. Expected WSO2Event, but found " + eventObject.getClass()
                    .getCanonicalName() + ". Hence, dropping the event");
        }
    }

    @Override
    protected boolean allowNullInTransportProperties() {
        return false;
    }

    public org.wso2.carbon.databridge.commons.StreamDefinition getWSO2StreamDefinition() {
        return this.streamDefinition;
    }

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{org.wso2.carbon.databridge.commons.Event.class};
    }

}
