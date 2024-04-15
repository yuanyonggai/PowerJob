package tech.powerjob.common.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class PlaceholderFrameworkUtilsTest {

    @BeforeEach
    void setUp() {
    }

    @Test // 正常
    public void testConvert_normal() {
        String templateContent = "${myParam} , Hello , ${StringFunction.reverse('Hello')}!";
        Map<String, String> binding = new HashMap<>();
        binding.put("myParam", "123456");

        String result = PlaceholderFrameworkUtils.convert(templateContent, binding);
        System.out.println(result);
    }

    @Test // 异常
    public void testConvert_exception() {

        String templateContent = "${myParam} , Hello , ${StringFunction.reverse('Hello')}!";

        Map<String, String> binding = new HashMap<>();
        try {
            String result = PlaceholderFrameworkUtils.convert(templateContent, binding);
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test // 逻辑表达式
    public void testConvert_express() {

        // 定义模板内容
        String templateContent = "Hello, ${name}! <% if (age >= 18) { %>You are an adult.<% } else { %>You are a minor.<% } %>";
        // 定义模板参数
        Map<String, Object> binding = new HashMap<>();
        binding.put("name", "John");
        binding.put("age", 25);

        String result = PlaceholderFrameworkUtils.convert(templateContent, binding);
        System.out.println(result);
    }

}