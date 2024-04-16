package tech.powerjob.samples.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

import tech.powerjob.common.utils.CommonUtils;

import com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceBuilder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.h2.Driver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import tech.powerjob.official.processors.impl.sql.SpringDatasourceSqlProcessor;
import tech.powerjob.samples.SampleApplication;

import javax.sql.DataSource;

/**
 * @author Echo009
 * @since 2021/3/10
 */
@Configuration
@ConditionalOnBean(SampleApplication.class)
public class SqlProcessorConfiguration {


    @Bean
    @DependsOn({"sampleApplication"})
    public DataSource sqlProcessorDataSource() {
        String path = System.getProperty("user.home") + "/test/h2/" + CommonUtils.genUUID() + "/";
        String jdbcUrl = String.format("jdbc:h2:file:%spowerjob_sql_processor_db;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false", path);
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(Driver.class.getName());
        config.setJdbcUrl(jdbcUrl);
        config.setAutoCommit(true);
        // 池中最小空闲连接数量
        config.setMinimumIdle(1);
        // 池中最大连接数量
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }

    @Bean
    @DependsOn({"sampleApplication"})
    @ConfigurationProperties("spring.datasource.dynamic.datasource.amldmp")
    public DataSource amldmpDataSource(){
        return DruidDataSourceBuilder.create().build();
    }

    @Bean
    @DependsOn({"sampleApplication"})
    @ConfigurationProperties("spring.datasource.dynamic.datasource.amldm")
    public DataSource amldmDataSource(){
        return DruidDataSourceBuilder.create().build();
    }

    @Bean("SpringDatasourceSqlProcessor")
    //修改：删除默认数据源
    public SpringDatasourceSqlProcessor simpleSpringSqlProcessor(DataSource amldmpDataSource, DataSource amldmDataSource) {
        SpringDatasourceSqlProcessor springDatasourceSqlProcessor = new SpringDatasourceSqlProcessor();
        //修改：添加多数据源
        springDatasourceSqlProcessor.registerDataSource(DataSourceNames.amldmp, amldmpDataSource);
        springDatasourceSqlProcessor.registerDataSource(DataSourceNames.amldm, amldmDataSource);
        // do nothing
        springDatasourceSqlProcessor.registerSqlValidator("fakeSqlValidator", sql -> true);
        // 排除掉包含 drop 的 SQL
        springDatasourceSqlProcessor.registerSqlValidator("interceptDropValidator", sql -> sql.matches("^(?i)((?!drop).)*$"));
        // do nothing
        springDatasourceSqlProcessor.setSqlParser((sql, taskContext) -> sql);
        return springDatasourceSqlProcessor;
    }
}
