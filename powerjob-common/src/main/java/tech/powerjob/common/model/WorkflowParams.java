package tech.powerjob.common.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import tech.powerjob.common.utils.CommonUtils;

@Data
@NoArgsConstructor
@Accessors(chain = true)
public class WorkflowParams {
    /**
     * 开始日期
     */
    private String dataDateStart;
    /**
     * 结束日期
     */
    private String dataDateEnd;

    public void valid() {
        CommonUtils.requireNonNull(this.dataDateStart, "dataDateStart can't be empty");
        CommonUtils.requireNonNull(this.dataDateEnd, "dataDateEnd can't be empty");
    }
}
