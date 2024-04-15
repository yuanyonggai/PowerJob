package tech.powerjob.common.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    @Test // 日期表达式
    public void testTime(){
        Map<String, Object> binding = new HashMap<>();
        binding.put("DataDate", "2024-04-15");     
        //${TIME('yyyy-MM-dd-1')}
        String templateContent = "${TIME.convert('yyyy-MM-dd')}";
        String result = PlaceholderFrameworkUtils.convert(templateContent, binding);
        System.out.println(result);

        templateContent = "${TIME.convert('week_end(yyyy-MM-dd,1)')}";
        result = PlaceholderFrameworkUtils.convert(templateContent, binding);
        System.out.println(result);

        templateContent = "${TIME.convert('timestamp(yyyyMMddHHmmss)')}";
        result = PlaceholderFrameworkUtils.convert(templateContent, binding);
        System.out.println(result);
    }

    @Test // 多法人过滤
    public void testRICD(){
        Map<String, Object> binding = new HashMap<>();
        List<String> reportBranchCodeList = new ArrayList<>();
        reportBranchCodeList.add("123");
        reportBranchCodeList.add("345");
        binding.put("reportBranchCodeList", reportBranchCodeList);     
        //${RICD('A.RICD',reportBranchCodeList)}
        String templateContent = "${RICD.convert('A.RICD')}";
        String result = PlaceholderFrameworkUtils.convert(templateContent, binding);
        System.out.println(result);        
    }

    @Test // 工作日函数
    public void testWorkDay(){
        Map<String, Object> binding = new HashMap<>();
        binding.put("DataDate", "2024-04-15");     
        //${WORK_DAY(10)}
        String templateContent = "${WORKDAY.convert(10)}";
        String result = PlaceholderFrameworkUtils.convert(templateContent, binding);
        System.out.println(result);        
    }

    @Test // 字典工作日函数
    public void testDictWorkDay(){
        Map<String, Object> binding = new HashMap<>();
        binding.put("DataDate", "2024-04-15");     
        //${WORK_DAY(10)}
        String templateContent = "${WORKDAY.convert(10)}";
        String result = PlaceholderFrameworkUtils.convert(templateContent, binding);
        System.out.println(result);        
    }

}