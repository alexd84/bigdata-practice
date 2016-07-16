package datagenerator;

import org.bson.types.ObjectId;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

class Auction {
    private final static Random rand = new Random();
    private final static SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");

    final String auctionId;
    final String date;
    final int publisherId;
    final int advertiserId;
    final int price;
    final boolean hasResponse;
    final boolean hasImpression;

    private Auction(String auctionId, String date, int publisherId, int advertiserId, int price, boolean hasResponse, boolean hasImpression) {
        this.auctionId = auctionId;
        this.date = date;
        this.publisherId = publisherId;
        this.advertiserId = advertiserId;
        this.price = price;
        this.hasResponse = hasResponse;
        this.hasImpression = hasImpression;
    }


    static Auction buildRandomAuction() {
        boolean hasResponse = rand.nextBoolean();
        boolean hasImpression = hasResponse && rand.nextBoolean();
        Date date = new Date(System.currentTimeMillis() - (rand.nextInt(100) * 86400000));
        return new Auction(new ObjectId().toString(), sdf.format(date), rand.nextInt(10) + 1, rand.nextInt(10) + 1, rand.nextInt(10000), hasResponse, hasImpression);
    }
}
