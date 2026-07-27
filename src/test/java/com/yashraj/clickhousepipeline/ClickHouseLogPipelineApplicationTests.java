package com.yashraj.clickhousepipeline;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Plain context-loads smoke test, in the default direct mode from
 * src/test/resources/application.properties. JdbcTemplate is mocked so this never attempts
 * a real ClickHouse connection - a genuine failure to wire the application context (a
 * missing bean, a broken @Value expression, a bad @ConditionalOnProperty) fails this test.
 */
@SpringBootTest
@DisplayName("Application context")
class ClickHouseLogPipelineApplicationTests {

    @MockitoBean
    private JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("the full Spring context loads successfully in direct mode")
    void contextLoads() {
        // Intentionally empty - a failed ApplicationContext startup fails this test on its own.
    }
}
