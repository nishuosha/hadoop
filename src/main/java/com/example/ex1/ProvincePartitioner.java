package com.example.ex1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    public static HashMap<String, Integer> provinceDict = new HashMap<>();

    static {
        provinceDict.put("123", 0);
        provinceDict.put("234", 1);
        provinceDict.put("456", 2);
    }

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        Integer provinceId = provinceDict.get(text.toString());
        return provinceId ==  null ? 3 : provinceId;
    }
}
