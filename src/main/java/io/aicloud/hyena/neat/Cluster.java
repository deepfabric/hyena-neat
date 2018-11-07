package io.aicloud.hyena.neat;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import netpart.node.Node;
import netpart.node.ServerNode;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Description:
 * <pre>
 * Date: 2018-10-24
 * Time: 8:29
 * </pre>
 *
 * @author fagongzi
 */
@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "cluster")
@Slf4j
public class Cluster implements InitializingBean {
    private Random random;
    private List<Server> targets = new ArrayList<>();
    private List<Node> nodes = new ArrayList<>();

    public List[] randomNodes() {
        int index = random.nextInt(nodes.size() - 1) + 1;
        return new List[]{
                nodes.subList(0, index), nodes.subList(index, nodes.size())};
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("start targets nodes: {}", targets);

        random = new Random(System.currentTimeMillis());
        targets.forEach(e -> nodes.add(e.toNode()));
    }

    @Setter
    @Getter
    public static class Server {
        private String name;
        private String host;
        private int port = 22;
        private String userName = "root";
        private String sshKey;

        private Node toNode() {
            return new ServerNode(name, host, port, userName, sshKey);
        }

        @Override
        public String toString() {
            return "Server{" +
                    "name='" + name + '\'' +
                    ", host='" + host + '\'' +
                    ", port=" + port +
                    ", userName='" + userName + '\'' +
                    ", sshKey='" + sshKey + '\'' +
                    '}';
        }
    }
}
