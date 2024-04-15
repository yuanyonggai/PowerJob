package tech.powerjob.common.utils.function;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import cn.hutool.core.collection.IterUtil;
import cn.hutool.core.util.StrUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import tech.powerjob.common.constants.CommonConstants;
import tech.powerjob.common.exception.PowerJobException;

@Data
@AllArgsConstructor
public class RICDFunction {
    private Map<String, Object> finalBinding;

    public String convert(String filedName) {
        if (StrUtil.isBlank(filedName)) {
            throw new PowerJobException("参数错误,表字段名为空");
        }
        List<String> reportBranchCodeList = null;
        if (finalBinding.get(CommonConstants.REPORT_BRANCH_CODE_LIST) != null) {
            reportBranchCodeList = (List<String>) finalBinding.get(CommonConstants.REPORT_BRANCH_CODE_LIST);
        } else {
            throw new PowerJobException("参数错误,报送机构编码列表为空");
        }

        List<String> reportBranchCodeNewList = reportBranchCodeList.stream().map(e -> {
            if (e == null || e.equals("null")) {
                return null;
            }
            return e;
        }).filter(Objects::nonNull).collect(Collectors.toList());

        if (reportBranchCodeNewList.size() == 0) {
            return " 1 = 1 ";
        }

        if (reportBranchCodeNewList.size() == 1) {
            return filedName + " = " + "'" + reportBranchCodeList.get(0) + "'";
        } else {
            return filedName + " in (" + IterUtil.join(reportBranchCodeNewList.iterator(), ",", "'", "'") + ")";
        }

    }
}
