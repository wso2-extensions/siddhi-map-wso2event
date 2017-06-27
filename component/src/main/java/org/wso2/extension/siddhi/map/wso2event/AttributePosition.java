package org.wso2.extension.siddhi.map.wso2event;


import java.util.Map;

/**
 * Class which keeps the attribute position (of Meta, Correlation and Payload) for WSO2Event mapping
 */
public class AttributePosition {

    private WSO2EventMapperUtils.InputDataType inputDataType;
    private Map<Integer, Integer> eventPositionMap;

    public WSO2EventMapperUtils.InputDataType getInputDataType() {
        return inputDataType;
    }

    public void setInputDataType(WSO2EventMapperUtils.InputDataType inputDataType) {
        this.inputDataType = inputDataType;
    }

    public Map<Integer, Integer> getEventPositionMap() {
        return eventPositionMap;
    }

    public void setEventPositionMap(Map<Integer, Integer> eventPositionMap) {
        this.eventPositionMap = eventPositionMap;
    }

    public AttributePosition(WSO2EventMapperUtils.InputDataType inputDataType, Map<Integer, Integer> eventPositionMap) {
        this.inputDataType = inputDataType;
        this.eventPositionMap = eventPositionMap;
    }
}
