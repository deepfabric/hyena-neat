package io.aicloud.hyena.neat.dateset;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Description:
 * <pre>
 * Date: 2018-11-07
 * Time: 16:41
 * </pre>
 *
 * @author fagongzi
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "dataset")
@Component
public class Dateset implements Reader {
    private InputStream in;
    private String file;
    private int dim;
    private int total;
    private int read;
    private ByteBuf buf;

    @Override
    public void reset() throws IOException {
        try {
            this.in = new FileInputStream(file);
            int size = in.available();
            byte[] data = new byte[in.available()];
            int n = this.in.read(data);
            if (n != data.length) {
                throw new RuntimeException("read data not complete");
            }

            buf = Unpooled.wrappedBuffer(data);

            buf.markReaderIndex();
            dim = buf.readIntLE();
            buf.resetReaderIndex();
            if (size % ((dim + 1) * 4) != 0) {
                throw new RuntimeException("weird file size: " + size + "," + dim);
            }

            total = size / ((dim + 1) * 4);
            read = 0;
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    @Override
    public Float[] read() throws IOException {
        Float[] value = new Float[dim];
        buf.skipBytes(4);
        for (int i = 0; i < dim; i++) {
            value[i] = buf.readFloatLE();
        }
        read++;
        return value;
    }

    @Override
    public boolean next() {
        return read < total;
    }
}
