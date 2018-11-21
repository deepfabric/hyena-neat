package io.aicloud.hyena.neat.tester;

import io.aicloud.hyena.neat.Cluster;
import io.aicloud.hyena.neat.dateset.Reader;
import io.aicloud.hyena.neat.util.PartitionFormat;
import io.aicloud.sdk.hyena.Builder;
import io.aicloud.sdk.hyena.Client;
import io.aicloud.sdk.hyena.pb.InsertRequest;
import io.aicloud.sdk.hyena.pb.SearchRequest;
import io.aicloud.sdk.hyena.pb.SearchResponse;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import netpart.partitioner.impl.iptables.IpTablesPartition;
import netpart.partitioner.impl.iptables.IpTablesPartitioner;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
    private Client client;
    private long count;
    private long timeout;

    @Autowired
    private Cluster cluster;
    @Autowired
    private Reader reader;
    private IpTablesPartition lastPartition;

    private ExecutorService executor = Executors.newScheduledThreadPool(1);
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private IpTablesPartitioner partitioner = new IpTablesPartitioner();

    private List<Float[]> records = new ArrayList<>();
    private List<Boolean> result = new ArrayList<>();

    private void startOp() throws IOException {
        for (; reader.next(); ) {
            records.add(reader.read());
        }

        log.info("start op with {} records", records.size());
        executor.execute(() -> {
            AtomicLong value = new AtomicLong(0);
            records.parallelStream().forEach(e -> {
                long newId, offset = 0;
                try {
                    newId = value.incrementAndGet();
                    if (newId >= count) {
                        log.info("{} reach max count {}, exit", newId, count);
                        System.exit(0);
                    }

                    client.insert(InsertRequest.newBuilder().addIds(newId).addAllXbs(Arrays.asList(e)).build());
                    io.aicloud.sdk.hyena.Future future = client.search(SearchRequest.newBuilder().addAllXq(Arrays.asList(e)).build());
                    SearchResponse resp = future.get();
                    if (resp.getXids(0) == -1) {
                        log.error("search {} failed with -1", offset);
                        System.exit(1);
                    }
                    if (resp.getXids(0) != newId) {
                        log.error("search {} failed with not match expect {}, got {}", offset, newId, resp.getXids(0));
                        System.exit(1);
                    }
                } catch (Exception e1) {
                    log.error("search {} failed", offset, e1);
                    System.exit(1);
                }
            });
        });
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

        initHyena();
        startOp();
        startRandomPartition();
    }

    private void initHyena() throws Exception {
        reader.reset();
        client = new Builder(hyena)
                .kafka(kafka, topic)
                .dim(reader.getDim())
                .timeout(timeout, TimeUnit.SECONDS)
                .build();
    }
}
