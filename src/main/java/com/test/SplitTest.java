package com.test;

public class SplitTest {

    public static void main(String[] args) {

        String s1 = "zhanghao,zhanghao";
        String s2 = "zhanghao";
        String s3 = "zhanghao，zhanghao";
        String s4 = "hao";

        String[] ss = s1.split("[,，]");

        System.out.println(s1.split("[,，]").length);
        System.out.println(s2.split("[,，]").length);
        System.out.println(s3.split("[,，]").length);
        System.out.println(s4.split(",").length);
    }

}
