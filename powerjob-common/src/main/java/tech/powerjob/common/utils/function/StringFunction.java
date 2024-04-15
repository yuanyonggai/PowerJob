package tech.powerjob.common.utils.function;

public class StringFunction {
    public static String reverse(String input) {
        return new StringBuilder(input).reverse().toString();
    }
}