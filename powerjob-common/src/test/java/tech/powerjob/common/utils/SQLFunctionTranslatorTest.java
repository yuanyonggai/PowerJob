package tech.powerjob.common.utils;

import org.junit.jupiter.api.Test;

import tech.powerjob.common.utils.db.func.SQLFunctionTranslator;

import java.util.HashMap;

public class SQLFunctionTranslatorTest {
    @Test
    public void test() {
        HashMap contextData = new HashMap();
        contextData.put("p_ss", "12345");
        String sqltemplate3 = "@{fc.getConcat(fc.nvlStr('trade_occur_country',' '),fc.nvlStr('trade_occur_area',' '))}";
        String sqltemplate1 = "${SQL.field_to_date_func('a.case_batch','yyyy-mm-dd')}";
        String sqltemplate = " @{fc.subPartitionBy('t.name','')}";
        String sqltemplate2 = " @{fc.prePartitionBy('t.name1',\"t.name\")}";

        String rs1 = SQLFunctionTranslator.trans("oracle", sqltemplate1, contextData);

        System.out.println(rs1);
    }
}
