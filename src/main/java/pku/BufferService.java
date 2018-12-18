package pku;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;

/**
 *
 * Created by yixu on 2018/12/11.
 */
public class BufferService {

    private volatile static BufferService instance;

    private final String storePath;
    private final HashMap<String, FileChannel> fileChannels = new HashMap<>(100);

    public BufferService(String storePath) {
        this.storePath = storePath;

        // In case the storePath does not exist
        // Paths.get(storePath).toFile().mkdirs();
    }


    public static BufferService getInstance(String storePath) {
        if (instance == null) {
            synchronized (BufferService.class) {
                if (instance == null) {
                    instance = new BufferService(storePath);
                }
            }
        }
        return instance;
    }

    public ByteBuffer getBuffer(String bucket) {

        FileChannel channel = fileChannels.get(bucket);
        if (channel == null) {
            synchronized (fileChannels) {
                if ((channel = fileChannels.get(bucket)) == null) {
                    try {
                        channel = FileChannel.open(Paths.get(storePath, bucket), StandardOpenOption.READ);
                    } catch (IOException ex) {
                        throw new RuntimeException("File channel open file failed", ex);
                    }
                    fileChannels.put(bucket, channel);
                }
            }
        }

        MappedByteBuffer buffer;
        try {
            buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        } catch (IOException ex) {
            throw new RuntimeException("File channel open file failed", ex);
        }

        return buffer;
    }
}
