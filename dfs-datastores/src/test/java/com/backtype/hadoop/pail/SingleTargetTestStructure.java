package com.backtype.hadoop.pail;


import java.util.ArrayList;
import java.util.List;

public class SingleTargetTestStructure implements PailStructure<String> {

    public boolean isValidTarget(String... dirs) {
            return dirs != null & dirs.length >= 1;
    }

    public String deserialize(byte[] serialized) {
            return new String(serialized);
    }

    public byte[] serialize(String object) {
            return object.getBytes();
    }

    public List<String> getTarget(String object) {
        List<String> target = new ArrayList<String>();
        target.add(object.charAt(0)+"");
        return target;
    }

    public Class getType() {
            return String.class;
    }
}