package datagenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import util.Settings;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Generator {
    private static final int RECORDS_COUNT = 10000;
    private final FileSystem fs;

    private Generator() throws URISyntaxException, IOException {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        fs = FileSystem.get(new URI(Settings.HDFS_ROOT), new Configuration());
    }

    public static void main(String[] args) throws Exception {
        Generator gen = new Generator();
        gen.cleanExisting();
        gen.generateFiles();
    }

    private void generateFiles() throws IOException {
        String reqTemplate = new String(Files.readAllBytes(Paths.get("src/main/resources/samples/AdRequest.json")));
        String rspTemplate = new String(Files.readAllBytes(Paths.get("src/main/resources/samples/AdResponse.json")));
        String impTemplate = new String(Files.readAllBytes(Paths.get("src/main/resources/samples/Impression.json")));

        StringBuilder sbReq = new StringBuilder();
        StringBuilder sbRsp = new StringBuilder();
        StringBuilder sbImp = new StringBuilder();

        for (int i = 0; i < RECORDS_COUNT; i++) {
            System.out.println(i);
            Auction auction = Auction.buildRandomAuction();
            String reqJson = reqTemplate
                    .replaceAll("\\$auctionId", auction.auctionId)
                    .replaceAll("\\$date", auction.date)
                    .replaceAll("\\$publisherId", String.valueOf(auction.publisherId))
                    .replaceAll("\\$advertiserId", String.valueOf(auction.advertiserId));
            sbReq.append(reqJson).append("\n");

            if (auction.hasResponse) {
                String rspJson = rspTemplate
                        .replaceAll("\\$auctionId", auction.auctionId)
                        .replaceAll("\\$price", String.valueOf(auction.price))
                        .replaceAll("\\$advertiserId", String.valueOf(auction.advertiserId));
                sbRsp.append(rspJson).append("\n");
            }

            if (auction.hasImpression) {
                String impJson = impTemplate
                        .replaceAll("\\$auctionId", auction.auctionId)
                        .replaceAll("\\$date", auction.date);
                sbImp.append(impJson).append("\n");
            }
        }

        System.out.println("writing to hdfs...");
        writeFile(new Path(Settings.HDFS_ROOT + "/user/bigdata/rtb/AdRequests/in.json"), sbReq.toString());
        writeFile(new Path(Settings.HDFS_ROOT + "/user/bigdata/rtb/AdResponses/in.json"), sbRsp.toString());
        writeFile(new Path(Settings.HDFS_ROOT + "/user/bigdata/rtb/Impressions/in.json"), sbImp.toString());
        writeFile(new Path(Settings.HDFS_ROOT + "/user/bigdata/wordcount/words.txt"), "dog cat rat \ncar car rat \ndog car cat");
        System.out.println("done!");
    }

    private void writeFile(Path path, String content) throws IOException {
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)));
        br.write(content);
        br.close();
    }

    private void cleanExisting() throws IOException {
        fs.delete(new Path(Settings.HDFS_ROOT + "/user/bigdata"), true);
    }

}
