package tech.powerjob.official.processors.impl.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import tech.powerjob.official.processors.impl.sql.SpringDatasourceSqlProcessor;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.sdk.BasicProcessor;
import tech.powerjob.worker.log.OmsLogger;

/**
 * common java processor
 *
 * @author yyg
 * @since 2024/4/11
 */
@Component("JavaDemoProcessor")
public class JavaDemoProcessor implements BasicProcessor {

    @Autowired
    public SpringDatasourceSqlProcessor springDatasourceSqlProcessor;

    @Override
    public ProcessResult process(TaskContext taskContext) throws Exception {
        OmsLogger omsLogger = taskContext.getOmsLogger();

        // java简单计算
        int i = 55;
        omsLogger.info("i:{}", i + 1);

        // jdbc测试
        String businessDate = "111";
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String url = "jdbc:oracle:thin:@//127.0.0.1:1521/orcl";
        String uname = "aml_dmp";
        String pwd = "aml_dmp";
        String sql = "select PROJECT_KEY from JOB_PROJECT where PROJECT_KEY= ?";
        Connection connection = DriverManager.getConnection(url, uname, pwd);
        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setString(1, businessDate);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String key = rs.getString("PROJECT_KEY");
                omsLogger.info("key:{}", key);
            }
        } catch (Exception e) {
            return new ProcessResult(false, "execute error");
        } finally {
            connection.close();
        }

        // 引用bean
        omsLogger.info("springDatasourceSqlProcessor:{}", springDatasourceSqlProcessor);
        return new ProcessResult(true, "execute successfully");
    }

}
