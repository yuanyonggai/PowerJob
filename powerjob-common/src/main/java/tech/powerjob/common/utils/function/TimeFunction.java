package tech.powerjob.common.utils.function;

import java.util.Date;
import java.util.Map;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.constants.CommonConstants;
import tech.powerjob.common.exception.PowerJobException;
import tech.powerjob.common.utils.TimePlaceholderUtils;

@Data
@AllArgsConstructor
@Slf4j
public class TimeFunction {
    private Map<String, Object> finalBinding;

    public String convert(String input) {
        Date date = new Date();
        if (StrUtil.isBlankIfStr(input)) {
            throw new PowerJobException("参数未输入错误");
        }
        if (finalBinding.get(CommonConstants.DATA_DATE) != null) {
            try {
                date = DateUtil.parse(String.valueOf(finalBinding.get(CommonConstants.DATA_DATE)));
            } catch (Exception e) {
                log.info(String.valueOf(finalBinding.get(CommonConstants.DATA_DATE)), e);
            }
        } else {
            throw new PowerJobException("参数DataDate为空错误");
        }
        return TimePlaceholderUtils.calculateTime(input, date);
    }
}
