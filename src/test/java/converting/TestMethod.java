package converting;

import org.junit.Test;

import java.io.IOException;

public class TestMethod {

    @Test
    public void testSimple() throws IOException {
        ConvertUtils convertUtils = new ConvertUtils();
        convertUtils.main(new String[]{"C:/Users/ekaterina_semenova/IdeaProjects/HDFS/src/test/resources/destinations.csv", "C:/Users/ekaterina_semenova/IdeaProjects/HDFS/src/test/resources/destinations.parquet"});

    }
}
