package tech.powerjob.common.utils.db.func.impl;

import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.utils.JobDateUtil;

import org.apache.commons.lang3.StringUtils;

@Slf4j
public class MysqlFunction extends AbstractSQLFunction {

    /////////    可以使用的方法  待实现    ///////////////////////////////////
    @Override
    public String db_date() {
        return "curdate()";
    }

    @Override
    public String now() {
        return "now()";
    }

    @Override
    public String str_to_date(String dateStr, String dateFormatterStr) {
        String sep = "'";
        if (dateStr.startsWith(COLUMN_FIELD_FLAG)) {
            dateStr = dateStr.substring(1);
            sep = "";
        }
        dateFormatterStr = getMysqlFormatString(dateFormatterStr);
        return dateFormatterStr != null ? "STR_TO_DATE(" + sep + dateStr + sep + ",'" + dateFormatterStr + "')" : "STR_TO_DATE(" + sep + dateStr + sep + ",'%Y-%m-%d')";
    }

    @Override
    public String find_in_set(String findStr, String setColumnName) {

        return "find_in_set('" + findStr + "'," + setColumnName + ")";
    }

    @Override
    public String from_db_table() {
        return "";
    }

    @Override
    public String row_num() {
        return " FETCH FIRST 1 ROWS ONLY ";
    }

    @Override
    public String truncate_by_table_name(String tablename) {
        return "truncate " + tablename;
    }

    @Override
    public String handle_insert_table(String tablename) {
        String sql = " into " + tablename;
        return sql;
    }

    @Override
    public String get_next_month_last_day(String date) {
        return JobDateUtil.getNextMonthLastDay(date);
    }

    @Override
    public String match_trade_ip() {
        return "REGEXP_LIKE(T.TRADE_IP,'([[:digit:]]{1,3})\\.([[:digit:]]{1,3})\\.([[:digit:]]{1,3})\\.([[:digit:]]{1,3})')";
    }

//    @Override
//    public String date_format_for_mybatis(String dateStrAndDateFormatterStr) {
//        String dateStr,  dateFormatterStr;
//        dateStr=dateStrAndDateFormatterStr.split("\\|")[0];
//        dateFormatterStr=dateStrAndDateFormatterStr.split("\\|")[1];
//        dateFormatterStr = getMysqlFormatString(dateFormatterStr);
//        return dateFormatterStr != null ? "STR_TO_DATE(" + dateStr + ",'" + dateFormatterStr + "')" : "STR_TO_DATE(" + dateStr + ",'%Y-%m-%d')";
//    }

    private String getMysqlFormatString(String dateFormatterStr) {
        if ("YYYY-MM-DD".equals(dateFormatterStr) || "yyyy-mm-dd".equals(dateFormatterStr) || "yyyy-MM-dd".equals(dateFormatterStr)) {
            dateFormatterStr = "%Y-%m-%d";
        } else if ("yyyy-MM-dd HH24:mi:ss".equals(dateFormatterStr)) {
            dateFormatterStr = "%Y-%m-%d %H:%i:%s";
        } else if ("yyyymmddhh24miss".equals(dateFormatterStr)) {
            dateFormatterStr = "%Y%m%d%H%i%s";
        }
        return dateFormatterStr;
    }

    @Override
    public String table(String tableAlias) {
        return tableAlias;
    }

    @Override
    public String interval_second(String second) {
        return "NOW() + interval " + second + " second";
    }

    @Override
    public String get_partition_by(String groupFields, String orderFields) {
        String restr = "ROW_NUMBER() OVER(PARTITION BY " + groupFields + " ORDER BY " + orderFields + ")";
        return restr;
    }

    @Override
    public String get_connect() {
        return "||";
    }

    @Override
    public String date_add(String dateStr, String dateFormatterStr, String days) {
        dateFormatterStr = getMysqlFormatString(dateFormatterStr);
        String sep = "'";
        if (dateStr.startsWith(COLUMN_FIELD_FLAG)) {
            dateStr = dateStr.substring(1);
            sep = "";
        }
        return dateFormatterStr != null ? "date_add(STR_TO_DATE(" + sep + dateStr + sep + ",'" + dateFormatterStr + "') , interval " + days + " day) " : "date_add(STR_TO_DATE(" + sep + dateStr + sep + ",'%Y-%m-%d')  , interval " + days + " day) ";

    }

    @Override
    public String date_format(String dateStr, String dateFormatterStr) {
        dateFormatterStr = getMysqlFormatString(dateFormatterStr);

        String sep = "'";
        if (dateStr.startsWith(COLUMN_FIELD_FLAG)) {
            dateStr = dateStr.substring(1);
            sep = "";
        }

        return dateFormatterStr != null ? "STR_TO_DATE(" + sep + dateStr + sep + ",'" + dateFormatterStr + "')" : "STR_TO_DATE(" + sep + dateStr + sep + ",'%Y-%m-%d')";
    }

    @Override
    public String field_to_date_func(String field, String dateFormatterStr) {
        return dateFormatterStr != null ? "STR_TO_DATE(" + field + ",'" + dateFormatterStr + "')" : "STR_TO_DATE(" + field + ",'%Y-%m-%d')";
    }

    @Override
    public String field_date_to_char_func(String dtdate, String dateFormatterStr) {
        dateFormatterStr = getMysqlFormatString(dateFormatterStr);
        return dateFormatterStr != null ? "DATE_FORMAT(" + dtdate + ",'" + dateFormatterStr + "')" : "DATE_FORMAT(" + dtdate + ",'%Y-%m-%d')";
    }

//    @Override
//    public String diff_date(String var1, String var2) {
//        return null;
//    }

    @Override
    public String group_concat(String str, String split) {
        return "group_concat(" + str + ",'" + split + "')";
    }

    /**
     * 分组排序取数sql
     *
     * @param groupFields 分组字段
     * @param orderFields 排序字段
     */
    @Override
    public String pre_partition_by(String groupFields, String orderFields) {
        String sql = "";

        if (StringUtils.isNotEmpty(groupFields)) {
            sql += "if(";
        }

        String[] groupFieldArray = groupFields.split(",");
        for (int i = 0; i < groupFieldArray.length; i++) {
            if (StringUtils.isNotEmpty(groupFields)) {
                sql += "@param" + i + "=" + groupFieldArray[i] + " and ";
            }
        }

        if (StringUtils.isNotEmpty(groupFields)) {
            sql = sql.substring(0, sql.length() - 4);
            sql += ",@rank:=@rank+1,@rank:=1) as PK,";
        } else {
            sql += "@rank:=@rank+1 as PK";
        }

        for (int i = 0; i < groupFieldArray.length; i++) {
            if (StringUtils.isNotEmpty(groupFields)) {
                sql += "@param" + i + ":=" + groupFieldArray[i] + ",";
            }
        }
        if (StringUtils.isNotEmpty(groupFields)) {
            sql = sql.substring(0, sql.length() - 1);
        }

        return sql;
    }

    /**
     * 分组排序取数sql
     *
     * @param groupFields 分组字段
     * @param orderFields 排序字段
     */
    @Override
    public String sub_partition_by(String groupFields, String orderFields) {
        String sql = ",(select @rank :=0";
        if (StringUtils.isNotEmpty(groupFields)) {
            sql += ",";
        }

        String[] groupFieldArray = groupFields.split(",");
        for (int i = 0; i < groupFieldArray.length; i++) {
            if (StringUtils.isNotEmpty(groupFields)) {
                sql += "@param" + i + ":= null,";
            }
        }
        if (StringUtils.isNotEmpty(groupFields)) {
            sql = sql.substring(0, sql.length() - 1);
        }

        sql += ") xx ";

        if (StringUtils.isNotEmpty(groupFields)) {
            sql += "ORDER BY " + groupFields;
        }

        if (StringUtils.isNotEmpty(orderFields)) {
            if (StringUtils.isNotEmpty(groupFields)) {
                sql += ",";
            } else {
                sql += "ORDER BY ";
            }
            sql += orderFields;
        }

        return sql;
    }

    ////////    以下方法不再使用     /////////////////////////////////////
//    @Override
//    public String ch2_num(String chstr) {
//        return "integer(" + chstr + ")";
//    }
//
//    @Override
//    public String first_row() {
//        return "fetch first 1 row only";
//    }
//
//    @Override
//    public String lock_row(String var1) {
//        return null;
//    }
//
//    public String lockrow(String sqlStr) {
//        return String.valueOf(sqlStr) + " FOR UPDATE WITH RR";
//    }
//
//    @Override
//    public String get_sequence_next_val(String seqname) {
//        String sql = " select NEXTVAL FOR " + seqname + " from sysibm.sysdummy1 ";
//        return sql;
//    }

    public String deleteTable(String tablename) {
        String sql = "delete from " + tablename;
        return sql;
    }

//    @Override
//    public String deal_inster_table(String tablename) {
//        String sql = " into " + tablename;
//        return sql;
//    }

    @Override
    public String rownum() {
        return "ROW_NUMBER() over()";
    }

    @Override
    public String get_begin_date(String granularity, String gradingDate) {
        return null;
    }


    @Override
    public String get_month_begin(String dateStr) {
        return null;
    }

    //    @Override
//    public String length_without_space(String str) {
//        String restr = "";
//        restr = "length(ltrim(rtrim(" + str + ")))";
//        return restr;
//    }
//
//    @Override
//    public String trim_last_chr(String str) {
//        String restr = "";
//        restr = "substr(" + str + ",1,length(" + str + ")-1)";
//        return restr;
//    }
//
//    @Override
//    public String trim(String str) {
//        String restr = "";
//        restr = "ltrim(rtrim(" + str + "))";
//        return restr;
//    }
//
//    @Override
//    public String num2char(String str) {
//        String restr = "";
//        restr = "replace(ltrim(replace(rtrim(char(" + str + ")),'0','   ')),'   ','0')";
//        return restr;
//    }
//
//    @Override
//    public String short_date_to8_char(String str) {
//        String restr = "";
//        restr = "replace(char(" + str + "),'-','')";
//        return restr;
//    }
//
//    @Override
//    public String to_number(String str) {
//        String sql = "decimal(" + str + ")";
//        return sql;
//    }
//
//    @Override
//    public String delete_repeat_record(String tablename, String primarykey) {
//        StringBuffer sql = new StringBuffer();
//        sql.append("delete from (select row_number() over (partition by ");
//        sql.append(primarykey);
//        sql.append(" order by ");
//        sql.append(primarykey);
//        sql.append(" ) as rn ,a.* from ");
//        sql.append(String.valueOf(tablename) + " a )");
//        sql.append(" where rn<>1 ");
//        return sql.toString();
//    }
//
//    @Override
//    public String add_day(String date, String days) {
//        return String.valueOf(this.date_format(date, "")) + days + " days ";
//    }
//
//    @Override
//    public String replace(String str, String org, String des) {
//        String restr = "";
//        restr = "replace(" + str + ",'" + org + "','" + des + "')";
//        return restr;
//    }
//
    public String to_char(String str) {
        String restr = "";
        restr = "CAST(" + str + " as CHAR)";
        return restr;
    }
}
