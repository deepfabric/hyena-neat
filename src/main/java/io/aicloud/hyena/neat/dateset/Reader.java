package io.aicloud.hyena.neat.dateset;

import java.io.IOException;

/**
 * Description:
 * <pre>
 * Date: 2018-11-07
 * Time: 16:43
 * </pre>
 *
 * @author fagongzi
 */
public interface Reader {
    void reset() throws IOException;

    float[] read() throws IOException;

    boolean next();

    void close() throws IOException;
}
