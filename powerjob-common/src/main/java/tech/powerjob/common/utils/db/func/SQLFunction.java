package tech.powerjob.common.utils.db.func;


import java.util.Map;

/**
 * sql 函数适配
 * 函数命名规范：
 * 1.遵循简短易懂、书写方便原则，使用小写字母、数字和下划线，不能用驼峰形式，不大写
 * 2.可如果原有系统的函数名符合规范1，可以优先使用原有系统的函数名
 * 3.
 */
public interface SQLFunction {

    String UNIMPLEMENTED_FUNCTION = "unimplemented_function";

    //sql适配函数中用于标记字段名还是常量，#开始表示为表字段
    String COLUMN_FIELD_FLAG="#";

    Map getContext();

    void setContext(Map context);


    /**
     * 数据库时间戳
     * (time)
     *
     * @return
     */
    String now();

    /**
     * 日期字符串格式转换(应对mybatis把字符串转换成问号的情况)
     *
     * @param dateStr          日期字符串
     * @param dateFormatterStr 日期转换格式
     * @return
     */

    /**
     * 字符串连接
     */
    String concat(String... vars);

    /**
     * 数据库时间戳按秒间隔时间
     *
     * @return
     */
    String interval_second(String second);


    /////////    可以使用的方法      ///////////////////////////////////

    String truncate_by_table_name(String tablename);


    String group_concat(String str, String split);

    String uuid();


    /**
     * 数据库日期
     *
     * @return
     */
    String db_date();

    /**
     * 数据库时间戳
     *
     * @return
     */
    String db_time_stamp();

    /**
     * 数据库时间戳按秒间隔时间
     *
     * @return
     */
    String str_to_date(String dateStr, String dateFormatterStr);


    /**
     * 数据库时间戳按秒间隔时间
     *
     * @return
     */
    String field_to_date_func(String field, String dateFormatterStr);

    /**
     * set集合（逗号隔开）中查找是否存在findStr
     * @param findStr
     * @param setColumnName
     * @return
     */
    String find_in_set(String findStr, String setColumnName);

    /**
     * 操作系统表的抽象函数
     *
     * @return
     */
    String from_db_table();

    /**
     * 相同记录多条，随机取一条
     *
     * @return
     */
    String row_num();

    /**
     * mybatis 转义
     *
     * @param str
     * @return
     */
    String escape_for_mybatis(String str);

    /**
     * 清空表数据
     *
     * @param tablename 数据库表名
     * @return
     */
    String delete_by_table_name(String tablename);

    /**
     * 正则表达式匹配发生交易ip
     *
     * @return
     */
    String match_trade_ip();

    /**
     * 处理sql插入符号 INTO
     *
     * @param tablename 数据库表名
     * @return
     */
    String handle_insert_table(String tablename);

    /**
     * 根据条件删除表数据
     *
     * @param tableName 数据库表名
     * @param arg1      字段值
     * @param arg2      字段名
     * @return
     */
    String del_table_by_condition(String tableName, String arg1, String arg2);

    /**
     * 根据条件删除表数据
     *
     * @param tableName 数据库表名
     * @param arg1      字段值
     * @param arg2      字段名
     * @param arg3      是否parquet分区表
     * @return
     */
    String del_table_by_condition(String tableName, String arg1, String arg2, String arg3);


    /**
     * 分组
     *
     * @param groupFields
     * @param orderFields
     * @return
     */
    String get_partition_by(String groupFields, String orderFields);

    /**
     * 两个字段做关连
     *
     * @return
     */
    String get_connect();

    /**
     * 用于兼容impala删除语法, delete A from tablename A
     * @return
     */
    String table(String tableAlias);

    /**
     * nvl(TRADE_GO_AREA,'000000') PJC10160
     *
     * @return
     */
    String nvl_str(String column, String value);

    /**
     * nvl(TRADE_GO_AREA,'000000') PJC10160
     *
     * @return
     */
    String nvl_number(String column, int value);

    /**
     * 取行数
     *
     * @return
     */
    String rownum();

    /**
     * 分组排序前部分
     *
     * @param groupFields 分组字段
     * @param orderFields 排序字段
     */
    String pre_partition_by(String groupFields, String orderFields);

    /**
     * 分组排序后部分
     *
     * @param groupFields 分组字段
     * @param orderFields 排序字段
     */
    String sub_partition_by(String groupFields, String orderFields);

    /**
     * 优化sql oracle useHash用法
     *
     * @param table1
     * @param table2
     * @return
     */
    String use_hash(String table1, String table2);

    /**
     * 根据粒度获取日、星期第一天、月第一天等
     *
     * @param granularity
     * @param gradingDate
     * @return
     */
    String get_begin_date(String granularity, String gradingDate);

    /**
     * 在dateStr上加days天
     *
     * @param dateStr
     * @param dateFormatterStr 格式
     * @param days
     * @return
     */
    String date_add(String dateStr, String dateFormatterStr, String days);

    /**
     * 获取当月第一天
     *
     * @param dateStr
     * @return
     */
    String get_month_begin(String dateStr);

    /**
     * 获取月最后一天
     *
     * @return
     */
    String get_next_month_last_day(String date);

    /**
     * 日期字符串格式转换
     *
     * @param dateStr          日期字符串
     * @param dateFormatterStr 日期转换格式
     * @return
     */
    String date_format(String dateStr, String dateFormatterStr);

    /**
     * 转字符对象
     *
     * @param fieldName   字段名
     * @return
     */
    String to_char(String fieldName);



    //以下方法写好注释否则不要启用

//    @Deprecated
//    String diff_date(String var1, String var2);
//
//    @Deprecated
//    String get_start_num_sql(String sql) throws Exception;
//
//    @Deprecated
//    String fsub_str(String str, int start, int index);
//
//    @Deprecated
//    String vsub_str(String str, int start, int index);
//
//    @Deprecated
//    String escape(String str);
//
//    @Deprecated
//    String ch2_num(String var1);
//
//    @Deprecated
//    String first_row();
//
//    @Deprecated
//    String lock_row(String var1);
//
//    @Deprecated
//    String get_sequence_next_val(String var1);
//
//    @Deprecated
//    String deal_inster_table(String var1);
//
//    @Deprecated
//    String length_without_space(String var1);
//
//    @Deprecated
//    String trim_last_chr(String var1);
//
//    @Deprecated
//    String trim(String var1);
//
//    @Deprecated
//    String num2char(String var1);
//
//    @Deprecated
//    String short_date_to8_char(String var1);
//
//    @Deprecated
//    String to_number(String var1);
//
//    @Deprecated
//    String delete_repeat_record(String var1, String var2);
//
//    @Deprecated
//    String add_day(String var1, String var2);
//
//    @Deprecated
//    String replace(String var1, String var2, String var3);

}
