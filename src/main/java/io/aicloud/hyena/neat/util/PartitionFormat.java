package io.aicloud.hyena.neat.util;

import netpart.partitioner.impl.iptables.IpTablesPartition;

/**
 * Description:
 * <pre>
 * Date: 2018-10-24
 * Time: 9:47
 * </pre>
 *
 * @author fagongzi
 */
public class PartitionFormat {
    public static String format(IpTablesPartition partition) {
        StringBuilder sb = new StringBuilder("{");

        partition.getGroups().forEach(g -> {
            sb.append("[");
            g.forEach(e -> {
                sb.append(e.getHost());
                sb.append(",");
            });
            sb.append("],");
        });

        return sb.append("}").toString();
    }
}
