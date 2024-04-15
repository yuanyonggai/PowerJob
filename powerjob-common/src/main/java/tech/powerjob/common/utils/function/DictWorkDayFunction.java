package tech.powerjob.common.utils.function;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;

import com.baomidou.dynamic.datasource.DynamicRoutingDataSource;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.text.StrFormatter;
import cn.hutool.core.util.StrUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.constants.Constants;
import tech.powerjob.common.exception.PowerJobException;
import tech.powerjob.common.utils.SpringUtils;

@Data
@AllArgsConstructor
@Slf4j
public class DictWorkDayFunction {
    private Map<String, Object> finalBinding;

    public String convert(String dictType, String dictValue) {
        String sql = "select limit_date  from (select to_char(date_info, 'yyyy-mm-dd') as limit_date, row_number() over(order by date_info {}) as rn  from td14_workday where date_info {} to_date('{}', 'yyyy-mm-dd')  and workday_status = '1')  where rn = {} ";
        String sql1 = "SELECT b.NAME AS DAY FROM DICT_TYPE a INNER JOIN DICT_VALUE b ON a.ID = b.TYPE_ID WHERE a.CODE = '{}' AND b.CODE = '{}'";

        DataSource plat_dataSource = SpringUtils.getBean(DynamicRoutingDataSource.class).getDataSource("aml-dmp");
        JdbcTemplate jdbcTemplatePlat = new JdbcTemplate(plat_dataSource);
        String dateString = "";

        if (StrUtil.isBlank(dictType)) {
            throw new PowerJobException("dictType参数未输入错误");
        }

        if (StrUtil.isBlank(dictValue)) {
            throw new PowerJobException("dictValue参数未输入错误");
        }

        Integer dateStep = 0;

        String dictSql = StrFormatter.format(sql1, dictType, dictValue);
        log.info(dictSql);
        dateStep = jdbcTemplatePlat.queryForObject(dictSql, Integer.class);

        if (finalBinding.get(Constants.DATA_DATE) != null) {
            try {
                dateString = String.valueOf(finalBinding.get(Constants.DATA_DATE));
            } catch (Exception e) {
                log.info(String.valueOf(finalBinding.get(Constants.DATA_DATE)), e);
            }
        } else {
            dateString = DateUtil.today();
        }

        String formatSql = "";
        if (dateStep == 0) {
            dateStep = 1;
            formatSql = StrFormatter.format(sql, "asc", ">=", dateString, dateStep);
        } else if (dateStep > 0) {
            formatSql = StrFormatter.format(sql, "asc", ">", dateString, dateStep);
        } else {
            formatSql = StrFormatter.format(sql, "desc", "<", dateString, Math.abs(dateStep));
        }
        log.info(formatSql);
        return jdbcTemplatePlat.queryForObject(formatSql, String.class);
    }

}
