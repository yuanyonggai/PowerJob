package tech.powerjob.common.utils;

import cn.hutool.core.util.StrUtil;
import tech.powerjob.common.enums.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

/**
 * parameter parse utils
 */
public class ParameterUtils {

    private static final Logger logger = LoggerFactory.getLogger(ParameterUtils.class);

    public static String convertParameterPlaceholders(String parameterString) {
        Map<String, String> parameterMap = new HashMap<>();
        return convertParameterPlaceholders(parameterString, parameterMap);
    }

    /**
     * convert parameters place holders
     *
     * @param parameterString
     * @param parameterMap
     * @return
     */
    public static String convertParameterPlaceholders(String parameterString, Map<String, String> parameterMap) {
        if (StrUtil.isEmpty(parameterString)) {
            return parameterString;
        }
        return PlaceholderFrameworkUtils.convert(parameterString, parameterMap);
    }

    /**
     * set in parameter
     *
     * @param index
     * @param stmt
     * @param dataType
     * @param value
     * @throws Exception
     */
    public static void setInParameter(int index, PreparedStatement stmt, DataType dataType, String value) throws Exception {
        if (dataType.equals(DataType.VARCHAR)) {
            stmt.setString(index, value);
        } else if (dataType.equals(DataType.INTEGER)) {
            stmt.setInt(index, Integer.parseInt(value));
        } else if (dataType.equals(DataType.LONG)) {
            stmt.setLong(index, Long.parseLong(value));
        } else if (dataType.equals(DataType.FLOAT)) {
            stmt.setFloat(index, Float.parseFloat(value));
        } else if (dataType.equals(DataType.DOUBLE)) {
            stmt.setDouble(index, Double.parseDouble(value));
        } else if (dataType.equals(DataType.DATE)) {
            stmt.setString(index, value);
        } else if (dataType.equals(DataType.TIME)) {
            stmt.setString(index, value);
        } else if (dataType.equals(DataType.TIMESTAMP)) {
            stmt.setString(index, value);
        } else if (dataType.equals(DataType.BOOLEAN)) {
            stmt.setBoolean(index, Boolean.parseBoolean(value));
        }
    }

    /**
     * handle escapes
     *
     * @param inputString
     * @return
     */
    public static String handleEscapes(String inputString) {

        if (StrUtil.isNotEmpty(inputString)) {
            return inputString.replace("%", "////%");
        }
        return inputString;
    }
}
