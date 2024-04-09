package tech.powerjob.server.common.utils;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.PropertyPlaceholderHelper;

import java.util.*;


/**
 * time place holder utils
 */
public class TimePlaceholderUtils {

//    public static void main(String[] args) {
//
//        String s=  ("week_end(yyyy-MM-dd HH:mm:ss SSS,1)");
//        System.out.println(calculateTime(s,new Date()));
//
//    }

    /**
     * 逗号 ,
     */
    public static final String COMMA = ",";
    public static final char SUBTRACT_CHAR = '-';
    public static final char ADD_CHAR = '+';
    public static final char MULTIPLY_CHAR = '*';
    public static final char DIVISION_CHAR = '/';
    public static final char LEFT_BRACE_CHAR = '(';
    public static final char RIGHT_BRACE_CHAR = ')';
    public static final String ADD_STRING = "+";
    public static final String MULTIPLY_STRING = "*";
    public static final String DIVISION_STRING = "/";
    public static final String LEFT_BRACE_STRING = "(";
    public static final char P = 'P';
    public static final char N = 'N';
    public static final String SUBTRACT_STRING = "-";
    public static final String PARAMETER_FORMAT_TIME = "yyyyMMddHHmmss";
    /**
     * month_begin
     */
    public static final String MONTH_BEGIN = "month_begin";
    /**
     * add_months
     */
    public static final String ADD_MONTHS = "add_months";
    /**
     * month_end
     */
    public static final String MONTH_END = "month_end";
    /**
     * week_begin
     */
    public static final String WEEK_BEGIN = "week_begin";
    /**
     * week_end
     */
    public static final String WEEK_END = "week_end";
    /**
     * timestamp
     */
    public static final String TIMESTAMP = "timestamp";

    private static final Logger logger = LoggerFactory.getLogger(TimePlaceholderUtils.class);

    /**
     * Prefix of the position to be replaced
     */
    public static final String placeholderPrefix = "$[";

    /**
     * The suffix of the position to be replaced
     */
    public static final String placeholderSuffix = "]";

    /**
     * Replaces all placeholders of format {@code ${name}} with the value returned
     * from the supplied {@link PropertyPlaceholderHelper.PlaceholderResolver}.
     *
     * @param value                          the value containing the placeholders to be replaced
     * @param date                           custom date
     * @param ignoreUnresolvablePlaceholders
     * @return the supplied value with placeholders replaced inline
     */
    public static String replacePlaceholders(String value, Date date, boolean ignoreUnresolvablePlaceholders) {
        PropertyPlaceholderHelper strictHelper = getPropertyPlaceholderHelper(false);
        PropertyPlaceholderHelper nonStrictHelper = getPropertyPlaceholderHelper(true);

        PropertyPlaceholderHelper helper = (ignoreUnresolvablePlaceholders ? nonStrictHelper : strictHelper);
        return helper.replacePlaceholders(value, new TimePlaceholderResolver(value, date));
    }

    /**
     * Creates a new {@code PropertyPlaceholderHelper} that uses the supplied prefix and suffix.
     *
     * @param ignoreUnresolvablePlaceholders indicates whether unresolvable placeholders should
     *                                       be ignored ({@code true}) or cause an exception ({@code false})
     */
    private static PropertyPlaceholderHelper getPropertyPlaceholderHelper(boolean ignoreUnresolvablePlaceholders) {
        return new PropertyPlaceholderHelper(placeholderPrefix, placeholderSuffix, null, ignoreUnresolvablePlaceholders);
    }


    /**
     * Placeholder replacement resolver
     */
    private static class TimePlaceholderResolver implements
            PropertyPlaceholderHelper.PlaceholderResolver {

        private final String value;

        private final Date date;

        public TimePlaceholderResolver(String value, Date date) {
            this.value = value;
            this.date = date;
        }

        @Override
        public String resolvePlaceholder(String placeholderName) {
            try {
                return calculateTime(placeholderName, date);
            } catch (Exception ex) {
                logger.error(String.format("resolve placeholder '%s' in [ %s ]", placeholderName, value), ex);
                return null;
            }
        }
    }


    /**
     * calculate time
     *
     * @param date date
     * @return calculate time
     */
    public static String calculateTime(String expression, Date date) {
        // After N years: $[add_months(yyyyMMdd,12*N)], the first N months: $[add_months(yyyyMMdd,-N)], etc
        String value;

        try {
            if (expression.startsWith(TIMESTAMP)) {
                String timeExpression = expression.substring(TIMESTAMP.length() + 1, expression.length() - 1);

                Map.Entry<Date, String> entry = calcTimeExpression(timeExpression, date);

                String dateStr = DateUtil.format(entry.getKey(), entry.getValue());

                Date timestamp = DateUtil.parse(dateStr, PARAMETER_FORMAT_TIME);

                value = String.valueOf(timestamp.getTime() / 1000);
            } else {
                Map.Entry<Date, String> entry = calcTimeExpression(expression, date);
                value = DateUtil.format(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }

        return value;
    }

    /**
     * calculate expression's value
     *
     * @param expression
     * @return
     */
    private static Integer calculate(String expression) {
        expression = StrUtil.trim(expression);
        expression = convert(expression);

        List<String> result = string2List(expression);
        result = convert2SuffixList(result);

        return calculate(result);
    }

    /**
     * Change the sign in the expression to P (positive) N (negative)
     *
     * @param expression
     * @return eg. "-3+-6*(+8)-(-5) -> S3+S6*(P8)-(S5)"
     */
    private static String convert(String expression) {
        char[] arr = expression.toCharArray();

        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == SUBTRACT_CHAR) {
                if (i == 0) {
                    arr[i] = N;
                } else {
                    char c = arr[i - 1];
                    if (c == ADD_CHAR || c == SUBTRACT_CHAR || c == MULTIPLY_CHAR || c == DIVISION_CHAR || c == LEFT_BRACE_CHAR) {
                        arr[i] = N;
                    }
                }
            } else if (arr[i] == ADD_CHAR) {
                if (i == 0) {
                    arr[i] = P;
                } else {
                    char c = arr[i - 1];
                    if (c == ADD_CHAR || c == SUBTRACT_CHAR || c == MULTIPLY_CHAR || c == DIVISION_CHAR || c == LEFT_BRACE_CHAR) {
                        arr[i] = P;
                    }
                }
            }
        }

        return new String(arr);
    }

    /**
     * to suffix expression
     *
     * @param srcList
     * @return
     */
    private static List<String> convert2SuffixList(List<String> srcList) {
        List<String> result = new ArrayList<>();
        Stack<String> stack = new Stack<>();

        for (int i = 0; i < srcList.size(); i++) {
            if (Character.isDigit(srcList.get(i).charAt(0))) {
                result.add(srcList.get(i));
            } else {
                switch (srcList.get(i).charAt(0)) {
                    case LEFT_BRACE_CHAR:
                        stack.push(srcList.get(i));
                        break;
                    case RIGHT_BRACE_CHAR:
                        while (!LEFT_BRACE_STRING.equals(stack.peek())) {
                            result.add(stack.pop());
                        }
                        stack.pop();
                        break;
                    default:
                        while (!stack.isEmpty() && compare(stack.peek(), srcList.get(i))) {
                            result.add(stack.pop());
                        }
                        stack.push(srcList.get(i));
                        break;
                }
            }
        }

        while (!stack.isEmpty()) {
            result.add(stack.pop());
        }

        return result;
    }

    /**
     * Calculate the suffix expression
     *
     * @param result
     * @return
     */
    private static Integer calculate(List<String> result) {
        Stack<Integer> stack = new Stack<>();
        for (int i = 0; i < result.size(); i++) {
            if (Character.isDigit(result.get(i).charAt(0))) {
                stack.push(Integer.parseInt(result.get(i)));
            } else {
                Integer backInt = stack.pop();
                Integer frontInt = 0;
                char op = result.get(i).charAt(0);

                if (!(op == P || op == N)) {
                    frontInt = stack.pop();
                }

                Integer res = 0;
                switch (result.get(i).charAt(0)) {
                    case P:
                        res = frontInt + backInt;
                        break;
                    case N:
                        res = frontInt - backInt;
                        break;
                    case ADD_CHAR:
                        res = frontInt + backInt;
                        break;
                    case SUBTRACT_CHAR:
                        res = frontInt - backInt;
                        break;
                    case MULTIPLY_CHAR:
                        res = frontInt * backInt;
                        break;
                    case DIVISION_CHAR:
                        res = frontInt / backInt;
                        break;
                    default:
                        break;
                }
                stack.push(res);
            }
        }

        return stack.pop();
    }

    /**
     * string to list
     *
     * @param expression
     * @return list
     */
    private static List<String> string2List(String expression) {
        List<String> result = new ArrayList<>();
        String num = "";
        for (int i = 0; i < expression.length(); i++) {
            if (Character.isDigit(expression.charAt(i))) {
                num = num + expression.charAt(i);
            } else {
                if (!num.isEmpty()) {
                    result.add(num);
                }
                result.add(expression.charAt(i) + "");
                num = "";
            }
        }

        if (!num.isEmpty()) {
            result.add(num);
        }

        return result;
    }

    /**
     * compare loginUser level
     *
     * @param peek
     * @param cur
     * @return true or false
     */
    private static boolean compare(String peek, String cur) {
        if (MULTIPLY_STRING.equals(peek) && (DIVISION_STRING.equals(cur) || MULTIPLY_STRING.equals(cur) || ADD_STRING.equals(cur) || SUBTRACT_STRING.equals(cur))) {
            return true;
        } else if (DIVISION_STRING.equals(peek) && (DIVISION_STRING.equals(cur) || MULTIPLY_STRING.equals(cur) || ADD_STRING.equals(cur) || SUBTRACT_STRING.equals(cur))) {
            return true;
        } else if (ADD_STRING.equals(peek) && (ADD_STRING.equals(cur) || SUBTRACT_STRING.equals(cur))) {
            return true;
        } else {
            return SUBTRACT_STRING.equals(peek) && (ADD_STRING.equals(cur) || SUBTRACT_STRING.equals(cur));
        }

    }


    /**
     * calculate time expresstion
     *
     * @return <date, date format>
     */
    private static Map.Entry<Date, String> calcTimeExpression(String expression, Date date) {
        Map.Entry<Date, String> resultEntry;

        if (expression.startsWith(ADD_MONTHS)) {
            resultEntry = calcMonths(expression, date);
        } else if (expression.startsWith(MONTH_BEGIN)) {
            resultEntry = calcMonthBegin(expression, date);
        } else if (expression.startsWith(MONTH_END)) {
            resultEntry = calcMonthEnd(expression, date);
        } else if (expression.startsWith(WEEK_BEGIN)) {
            resultEntry = calcWeekStart(expression, date);
        } else if (expression.startsWith(WEEK_END)) {
            resultEntry = calcWeekEnd(expression, date);
        } else {
            resultEntry = calcMinutes(expression, date);
        }

        return resultEntry;
    }

    /**
     * get first day of month
     *
     * @return
     */
    private static Map.Entry<Date, String> calcMonthBegin(String expression, Date date) {
        String addMonthExpr = expression.substring(MONTH_BEGIN.length() + 1, expression.length() - 1);
        String[] params = addMonthExpr.split(COMMA);

        if (params.length == 2) {
            String dateFormat = params[0];
            String dayExpr = params[1];
            Integer day = calculate(dayExpr);
            Date targetDate = DateUtil.beginOfMonth(date);
            targetDate = DateUtil.offsetDay(targetDate, day);

            return new AbstractMap.SimpleImmutableEntry<>(targetDate, dateFormat);
        }

        throw new RuntimeException("expression not valid");
    }

    /**
     * get last day of month
     */
    private static Map.Entry<Date, String> calcMonthEnd(String expression, Date date) {
        String addMonthExpr = expression.substring(MONTH_END.length() + 1, expression.length() - 1);
        String[] params = addMonthExpr.split(COMMA);

        if (params.length == 2) {
            String dateFormat = params[0];
            String dayExpr = params[1];
            Integer day = calculate(dayExpr);
            Date targetDate = DateUtil.endOfMonth(date);
            targetDate = DateUtil.offsetDay(targetDate, day);

            return new AbstractMap.SimpleImmutableEntry<>(targetDate, dateFormat);
        }

        throw new RuntimeException("expression not valid");
    }

    /**
     * get first day of week
     *
     * @return monday
     */
    private static Map.Entry<Date, String> calcWeekStart(String expression, Date date) {
        String addMonthExpr = expression.substring(WEEK_BEGIN.length() + 1, expression.length() - 1);
        String[] params = addMonthExpr.split(COMMA);

        if (params.length == 2) {
            String dateFormat = params[0];
            String dayExpr = params[1];
            Integer day = calculate(dayExpr);
            Date targetDate = DateUtil.beginOfWeek(date);
            targetDate = DateUtil.offsetDay(targetDate, day);
            return new AbstractMap.SimpleImmutableEntry<>(targetDate, dateFormat);
        }

        throw new RuntimeException("expression not valid");
    }

    /**
     * get last day of week
     */
    private static Map.Entry<Date, String> calcWeekEnd(String expression, Date date) {
        String addMonthExpr = expression.substring(WEEK_END.length() + 1, expression.length() - 1);
        String[] params = addMonthExpr.split(COMMA);

        if (params.length == 2) {
            String dateFormat = params[0];
            String dayExpr = params[1];
            Integer day = calculate(dayExpr);
            Date targetDate = DateUtil.endOfWeek(date);

            targetDate = DateUtil.offsetDay(targetDate, day);

            return new AbstractMap.SimpleImmutableEntry<>(targetDate, dateFormat);
        }

        throw new RuntimeException("Expression not valid");
    }

    /**
     * calc months expression
     *
     * @return <date, format>
     */
    private static Map.Entry<Date, String> calcMonths(String expression, Date date) {
        String addMonthExpr = expression.substring(ADD_MONTHS.length() + 1, expression.length() - 1);
        String[] params = addMonthExpr.split(COMMA);

        if (params.length == 2) {
            String dateFormat = params[0];
            String monthExpr = params[1];
            Integer addMonth = calculate(monthExpr);
            //Date targetDate = org.apache.commons.lang3.time.DateUtils.addMonths(date, addMonth);
            Date targetDate = DateUtil.offsetMonth(date, addMonth);

            return new AbstractMap.SimpleImmutableEntry<>(targetDate, dateFormat);
        }

        throw new RuntimeException("expression not valid");
    }

    /**
     * calculate time expression
     *
     * @return <date, format>
     */
    private static Map.Entry<Date, String> calcMinutes(String expression, Date date) {
        if (expression.contains("+")) {
            int index = expression.lastIndexOf('+');

            if (Character.isDigit(expression.charAt(index + 1))) {
                String addMinuteExpr = expression.substring(index + 1);
                Date targetDate = DateUtil.offsetMinute(date, calcMinutes(addMinuteExpr));
                String dateFormat = expression.substring(0, index);

                return new AbstractMap.SimpleImmutableEntry<>(targetDate, dateFormat);
            }
        } else if (expression.contains("-")) {
            int index = expression.lastIndexOf('-');

            if (Character.isDigit(expression.charAt(index + 1))) {
                String addMinuteExpr = expression.substring(index + 1);
                Date targetDate = DateUtil.offsetMinute(date, 0 - calcMinutes(addMinuteExpr));
                String dateFormat = expression.substring(0, index);

                return new AbstractMap.SimpleImmutableEntry<>(targetDate, dateFormat);
            }

            // yyyy-MM-dd/HH:mm:ss
            return new AbstractMap.SimpleImmutableEntry<>(date, expression);
        }

        // $[HHmmss]
        return new AbstractMap.SimpleImmutableEntry<>(date, expression);
    }

    /**
     * calculate need minutes
     *
     * @param minuteExpression
     * @return
     */
    private static Integer calcMinutes(String minuteExpression) {
        int index = minuteExpression.indexOf("/");

        String calcExpression;

        if (index == -1) {
            calcExpression = String.format("60*24*(%s)", minuteExpression);
        } else {

            calcExpression = String.format("60*24*(%s)%s", minuteExpression.substring(0, index),
                    minuteExpression.substring(index));
        }

        return calculate(calcExpression);
    }

}
