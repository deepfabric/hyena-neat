package io.aicloud.hyena.neat.dateset;

import com.google.common.io.LittleEndianDataInputStream;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.DataInput;
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
    private DataInput in;
    private String file;
    private int dim;
    private int total;
    private int read;

    @Override
    public void reset() throws IOException {
        close();

        InputStream in = new FileInputStream(file);
        this.in = new LittleEndianDataInputStream(in);

        int size = in.available();
        dim = this.in.readInt();
        if (size % ((dim + 1) * 4) != 0) {
            throw new RuntimeException("weird file size: " + size + "," + dim);
        }

        total = size / ((dim + 1) * 4);
        read = 0;
    }

    @Override
    public float[] read() throws IOException {
        float[] value = new float[dim];
        for (int i = 0; i < dim; i++) {
            value[i] = in.readFloat();
        }
        read++;
        return value;
    }

    @Override
    public boolean next() {
        return read < total;
    }

    @Override
    public void close() throws IOException {
        if (in != null && in instanceof InputStream) {
            ((InputStream) in).close();
            in = null;
        }
    }
}
