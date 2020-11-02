package flink_udf;

import org.apache.flink.table.functions.ScalarFunction;

public class StringConcatUDF extends ScalarFunction {

    public String eval(String value1){

        return value1 + "_xx";
    }

    public String eval(String value1, String value2){

        return value1 + "_xx_" + value2 + "_yy";
    }

}
