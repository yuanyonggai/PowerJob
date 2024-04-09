package tech.powerjob.common.model;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import tech.powerjob.common.utils.CommonUtils;

@Data
@NoArgsConstructor
@Accessors(chain = true)
public class RunParams {
    /**
     * 数据日期
     */
    private String dataDate;
    /**
     * 开始日期
     */
    private String dataDateStart;
    /**
     * 结束日期
     */
    private String dataDateEnd;

    public void valid() {
        if (StringUtils.isBlank(dataDate)) {
            CommonUtils.requireNonNull(this.dataDateStart, "dataDateStart can't be empty");
            CommonUtils.requireNonNull(this.dataDateEnd, "dataDateEnd can't be empty");
        } else {
            this.dataDateStart = this.dataDate;
            this.dataDateEnd = this.dataDate;
        }
    }
}