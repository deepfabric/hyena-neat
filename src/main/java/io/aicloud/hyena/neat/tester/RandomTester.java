package io.aicloud.hyena.neat.tester;

import io.aicloud.hyena.neat.Cluster;
import io.aicloud.hyena.neat.util.PartitionFormat;
import io.aicloud.sdk.hyena.Builder;
import io.aicloud.sdk.hyena.Client;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import netpart.partitioner.impl.iptables.IpTablesPartition;
import netpart.partitioner.impl.iptables.IpTablesPartitioner;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Description:
 * <pre>
 * Date: 2018-10-24
 * Time: 8:28
 * </pre>
 *
 * @author fagongzi
 */
@Getter
@Setter
@Component
@Slf4j(topic = "random-tester")
@ConfigurationProperties(prefix = "tester.random")
public class RandomTester implements InitializingBean {
    private int period;
    private String[] hyena;
    private String kafka;
    private String topic;
    private int dim;
    private Client client;

    @Autowired
    private Cluster cluster;
    private IpTablesPartition lastPartition;

    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private IpTablesPartitioner partitioner = new IpTablesPartitioner();

    private void startOp() {
        log.info("start op");
    }

    private void startRandomPartition() {
        executorService.scheduleAtFixedRate(this::doPartition, period, period, TimeUnit.SECONDS);
    }

    private void doPartition() {
        if (null == lastPartition) {
            lastPartition = partitioner.fullPartition(cluster.randomNodes());
            log.info("start network full partition between {}", PartitionFormat.format(lastPartition));
        } else {
            partitioner.heal(lastPartition);
            log.info("resume network full partition between {}", PartitionFormat.format(lastPartition));
            lastPartition = null;
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("ready to exit");
            executorService.shutdown();
            log.info("thread pool is already shutdown");
            if (null != lastPartition) {
                partitioner.heal(lastPartition);
                log.info("resume network full partition between {}", PartitionFormat.format(lastPartition));
            }

            log.info("exit hook complete");
        }));

        startRandomPartition();
        startOp();
    }

    private void initHyena() throws Exception {
        client = new Builder(hyena)
                .kafka(kafka, topic)
                .dim(dim).build();
    }
}
