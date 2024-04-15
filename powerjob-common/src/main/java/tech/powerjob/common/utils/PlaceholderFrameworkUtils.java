package tech.powerjob.common.utils;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.exceptions.ExceptionUtil;

import groovy.text.SimpleTemplateEngine;
import groovy.text.Template;
import groovy.text.TemplateEngine;
import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.utils.db.dialect.Dialect;
import tech.powerjob.common.utils.function.StringFunction;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * 占位符替换工具类
 */
@Slf4j
public class PlaceholderFrameworkUtils {

    private static TemplateEngine engine = null;

    static {
        // 创建 Groovy 模板引擎
        engine = new SimpleTemplateEngine();
    }    

    public static String convert(String templateContent, Map<String, ?> binding) {
        StringWriter writer = new StringWriter();
        try {
            // 将外部传入的参数注册到 Binding 中
            Map<String, Object> finalBinding = new HashMap<>();
            if (CollectionUtil.isNotEmpty(binding)) {
                finalBinding.putAll(binding);
            }
            //将自定义函数注册到 Binding 中
            finalBinding.put("StringFunction", new StringFunction());

            if (binding.get(Dialect.class.getCanonicalName()) != null) {
                Dialect dialect = (Dialect) binding.get(Dialect.class.getCanonicalName());
                finalBinding.put("SQL", dialect.getFunc());
            }

            // 使用模板引擎加载模板文件
            Template template = engine.createTemplate(templateContent);

            template.make(finalBinding).writeTo(writer);

        } catch (Exception e) {

            ExceptionUtil.wrapRuntimeAndThrow(handleTemplateError(e, templateContent));
        }
        return writer.toString();
    }

    public static String convert(String templateContent) {
        return convert(templateContent, null);
    }

    private static String handleTemplateError(Exception e, String templateContent) {
        String errorMessage = ExceptionUtil.getSimpleMessage(e);
        errorMessage = errorMessage.replaceFirst("for class: .*", "");
        errorMessage += " || Template content:[ " + templateContent + " ]";
        return errorMessage;
    }
}
