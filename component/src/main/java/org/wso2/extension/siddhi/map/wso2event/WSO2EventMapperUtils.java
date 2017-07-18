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

import org.wso2.carbon.databridge.commons.AttributeType;
import org.wso2.carbon.databridge.commons.StreamDefinition;
import org.wso2.carbon.databridge.commons.exception.MalformedStreamDefinitionException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.List;

/**
 * Util class for WSO2EventMapper.
 */
public class WSO2EventMapperUtils {
    public static final String STREAM_NAME_VER_DELIMITER = ":";
    public static final String DEFAULT_STREAM_VERSION = "1.0.0";
    public static final String META_DATA_PREFIX = "meta_";
    public static final String CORRELATION_DATA_PREFIX = "correlation_";
    public static final String ARBITRARY_DATA_PREFIX = "arbitrary_";
    public static final String CUSTOM_MAPPING_ENABLED = "enable.custom.mapping";

    /**
     * Convert the given {@link Attribute} to WSO2 {@link org.wso2.carbon.databridge.commons.Attribute}.
     *
     * @param attributeName Siddhi Event attribute name
     * @param attributeType Siddhi Event attribute type
     * @return the created WSO2 Event attribute
     */
    public static org.wso2.carbon.databridge.commons.Attribute createWso2EventAttribute(
                                                                String attributeName, Attribute.Type attributeType) {
        org.wso2.carbon.databridge.commons.AttributeType dbAttributeType;
        switch (attributeType) {
            case BOOL:
                dbAttributeType = AttributeType.BOOL;
                break;
            case STRING:
                dbAttributeType = AttributeType.STRING;
                break;
            case INT:
                dbAttributeType = AttributeType.INT;
                break;
            case LONG:
                dbAttributeType = AttributeType.LONG;
                break;
            case FLOAT:
                dbAttributeType = AttributeType.FLOAT;
                break;
            case DOUBLE:
                dbAttributeType = AttributeType.DOUBLE;
                break;
            default:
                throw new SiddhiAppCreationException("Attribute type is not valid when converting to data bridge " +
                        "attribute. Found attribute,  Name : '" + attributeName + "', " +
                        "Type: '" + attributeType + "'");
        }

        return new org.wso2.carbon.databridge.commons.Attribute(attributeName, dbAttributeType);
    }

    public static StreamDefinition createWSO2EventStreamDefinition(
            String streamName, List<org.wso2.carbon.databridge.commons.Attribute> metaAttributeList,
            List<org.wso2.carbon.databridge.commons.Attribute> correlationAttributeList,
            List<org.wso2.carbon.databridge.commons.Attribute> payloadAttributeList)
            throws MalformedStreamDefinitionException {
        org.wso2.carbon.databridge.commons.StreamDefinition wso2StreamDefinition = new org.wso2.carbon.databridge
                .commons.StreamDefinition(streamName, DEFAULT_STREAM_VERSION);
        wso2StreamDefinition.setMetaData(metaAttributeList);
        wso2StreamDefinition.setCorrelationData(correlationAttributeList);
        wso2StreamDefinition.setPayloadData(payloadAttributeList);
        return wso2StreamDefinition;
    }

}
