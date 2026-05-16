import org.apache.hadoop.fs.FSDataInputStream;
import java.io.*;

public class MyFSDataInputStream implements AutoCloseable { 
    private BufferedReader reader;
    private FSDataInputStream rawIn;

    public MyFSDataInputStream(FSDataInputStream in) {
        this.rawIn = in;
        this.reader = new BufferedReader(new InputStreamReader(in));
    }

    public String readLine() throws IOException {
        return reader.readLine();
    }

    public void close() throws IOException {
        reader.close();
        rawIn.close();
    }
}