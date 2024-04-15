package tech.powerjob.common.utils.function;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;

import com.baomidou.dynamic.datasource.DynamicRoutingDataSource;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.text.StrFormatter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.constants.CommonConstants;
import tech.powerjob.common.exception.PowerJobException;
import tech.powerjob.common.utils.SpringUtils;

@Data
@AllArgsConstructor
@Slf4j
public class WorkDayFunction {
    private Map<String, Object> finalBinding;

    public String convert(Integer dateStep) {
        String sql = "select limit_date  from (select to_char(date_info, 'yyyy-mm-dd') as limit_date, row_number() over(order by date_info {}) as rn  from td14_workday where date_info {} to_date('{}', 'yyyy-mm-dd')  and workday_status = '1')  where rn = {} ";

        if (dateStep == null) {
            throw new PowerJobException("参数未输入错误");
        }

        if (SpringUtils.getBean(DynamicRoutingDataSource.class) == null) {
            throw new PowerJobException("数据源为空");
        }

        //TODO 待修改
        DataSource plat_dataSource = SpringUtils.getBean(DynamicRoutingDataSource.class).getDataSource("aml-dmp");

        JdbcTemplate jdbcTemplatePlat = new JdbcTemplate(plat_dataSource);
        String dateString = "";

        if (finalBinding.get(CommonConstants.DATA_DATE) != null) {
            try {
                dateString = String.valueOf(finalBinding.get(CommonConstants.DATA_DATE));
            } catch (Exception e) {
                log.info(String.valueOf(finalBinding.get(CommonConstants.DATA_DATE)), e);
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
