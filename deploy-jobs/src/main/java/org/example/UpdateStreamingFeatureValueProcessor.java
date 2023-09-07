package org.example;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.map.EntryProcessor;

import java.util.Map;

public class UpdateStreamingFeatureValueProcessor implements EntryProcessor<String, HazelcastJsonValue, Integer> {

    private long longValue;
    private String featureName;
    private float floatValue;
    private double doubleValue;
    private String stringValue;
    private int intValue;

    private enum ValueType {
        DOUBLE_VALUE ("double"),
        FLOAT_VALUE ("float"),
        LONG_VALUE ("long"),
        INT_VALUE ("int"),
        STRING_VALUE ("string");
        private String type;

        ValueType(String type) {
            this.type = type;
        }
        public String getType() {
            return type;
        }
    }
    private ValueType valuetype;

    public UpdateStreamingFeatureValueProcessor(String featureName, long longValue) {
        this.longValue = longValue;
        this.featureName = featureName;
        this.valuetype = ValueType.LONG_VALUE;
    }

    public UpdateStreamingFeatureValueProcessor(String featureName, float floatValue) {
        this.floatValue = floatValue;
        this.featureName = featureName;
        this.valuetype = ValueType.FLOAT_VALUE;
    }
    public UpdateStreamingFeatureValueProcessor(String featureName, double doubleValue) {
        this.doubleValue = doubleValue;
        this.featureName = featureName;
        this.valuetype = ValueType.DOUBLE_VALUE;
    }
    public UpdateStreamingFeatureValueProcessor(String featureName, int intValue) {
        this.intValue = intValue;
        this.featureName = featureName;
        this.valuetype = ValueType.INT_VALUE;
    }

    @Override
    public Integer process(Map.Entry<String, HazelcastJsonValue> entry) {

        //read existing streamingFeature values from map
        JsonObject existingStreamingFeatureValuesJson = getJsonObject(entry.getValue());
        //Update individual feature value (based on feature type)
        if (valuetype.equals(ValueType.DOUBLE_VALUE))
            existingStreamingFeatureValuesJson.set(featureName, doubleValue);
        if (valuetype.equals(ValueType.LONG_VALUE))
            existingStreamingFeatureValuesJson.set(featureName, longValue);
        if (valuetype.equals(ValueType.INT_VALUE))
            existingStreamingFeatureValuesJson.set(featureName, intValue);
        if (valuetype.equals(ValueType.STRING_VALUE))
            existingStreamingFeatureValuesJson.set(featureName, stringValue);
        //update the streamingFeature values entry in map
        entry.setValue(new HazelcastJsonValue(existingStreamingFeatureValuesJson.toString()));

        return null;

    }
    private static JsonObject getJsonObject(HazelcastJsonValue existingValue) {
        return (existingValue==null) ? new JsonObject() : new JsonObject(Json.parse(existingValue.toString()).asObject()) ;
    }
}
