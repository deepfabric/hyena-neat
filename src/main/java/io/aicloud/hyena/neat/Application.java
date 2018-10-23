package io.aicloud.hyena.neat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Description:
 * <pre>
 * Date: 2018-10-23
 * Time: 10:48
 * </pre>
 *
 * @author fagongzi
 */
@SpringBootApplication

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        if (logger.isInfoEnabled()) {
            logger.info("Application start.");
        }

        SpringApplication.run(Application.class, args);
    }
}
