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
import org.wso2.extension.siddhi.map.wso2event.service.StreamDefinitionHolder;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.List;

class Utils {
    static final String STREAM_NAME_VER_DELIMITER = ":";
    static final String WSO2_STREAM_ID_PARAMETER_NAME = "wso2event.stream.id";
    static final String ARBITRARY_MAP_ATTRIBUTE_PARAMETER_NAME = "arbitrary.map";
    static final String META_DATA_PREFIX = "meta_";
    static final String CORRELATION_DATA_PREFIX = "correlation_";
    /**
     * Convert the given {@link Attribute} to WSO2 {@link org.wso2.carbon.databridge.commons.Attribute}
     *
     * @param attribute Siddhi Event attribute object
     * @return the created WSO2 Event attribute
     */
    static org.wso2.carbon.databridge.commons.Attribute createWso2EventAttribute(Attribute attribute) {
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

    static StreamDefinition createWSO2EventStreamDefinition(
            String streamId, List<org.wso2.carbon.databridge.commons.Attribute> metaAttributeList,
            List<org.wso2.carbon.databridge.commons.Attribute> correlationAttributeList,
            List<org.wso2.carbon.databridge.commons.Attribute> payloadAttributeList)
            throws MalformedStreamDefinitionException {
        String streamNameVersion[] = streamId.split(STREAM_NAME_VER_DELIMITER);
        org.wso2.carbon.databridge.commons.StreamDefinition wso2StreamDefinition = new org.wso2.carbon.databridge
                .commons.StreamDefinition(streamNameVersion[0], streamNameVersion[1]);
        wso2StreamDefinition.setMetaData(metaAttributeList);
        wso2StreamDefinition.setCorrelationData(correlationAttributeList);
        wso2StreamDefinition.setPayloadData(payloadAttributeList);
        StreamDefinitionHolder.setStreamDefinition(wso2StreamDefinition);
        return wso2StreamDefinition;
    }

    enum InputDataType {
        META_DATA, CORRELATION_DATA, PAYLOAD_DATA
    }
}
