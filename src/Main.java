// 5.30.2023

import java.util.*;

import com.mongodb.client.*;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

/**
 * Run both the crawler and scraper and display their results periodically.
 */
public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        final String initialUrl = "https://www.touro.edu";
        final String domain = "touro.edu";

        final int DISPLAY_COLUMN_WIDTH = 168;
        final int DISPLAY_INTERVAL_MS = 60000;
        final int DISPLAY_INTERVAL_ITERATIONS = 10;

        // Get or create the db
        final MongoClient MONGO_CLIENT = MongoClients.create();
        // "." and "#" are invalid characters for db names in mongoDB
        String dbName = domain.replace(".", "_").replace("/", "#") + "-Data";
        final MongoDatabase DB = MONGO_CLIENT.getDatabase(dbName);
        final MongoCollection<org.bson.Document> SCRAPER_DATA_COLLECTION = DB.getCollection("scraperData");
        final MongoCollection<org.bson.Document> SCRAPER_URLS_COLLECTION = DB.getCollection("scraperUrls");
        final MongoCollection<org.bson.Document> CRAWLER_URLS_COLLECTION = DB.getCollection("crawlerUrls");
        IndexOptions indexOptions = new IndexOptions().unique(true);
        SCRAPER_DATA_COLLECTION.createIndex(Indexes.ascending("value", "type"), indexOptions);
        SCRAPER_URLS_COLLECTION.createIndex(Indexes.ascending("type"), indexOptions);
        CRAWLER_URLS_COLLECTION.createIndex(Indexes.ascending("type"), indexOptions);

        /* After the crawler is given the initial page url, each next page is processed with the scraper and crawler's
         * respective processNext() calls. Each will process the single next page in their respective queues.
         * Crawler's processNext() method must be called first to download the initial page, otherwise scraper would have
         * nothing to scrape). The crawler will then give the scraper its next page to process.
         * In a similar manner, Scraper will then give crawler its next url(s).
         * */
        Crawler crawler = new Crawler(initialUrl, CRAWLER_URLS_COLLECTION);
        Scraper scraper = new Scraper(domain, SCRAPER_DATA_COLLECTION, SCRAPER_URLS_COLLECTION);

        int numProcessedPages = 0;
        long lastDisplayTimeEnd = System.currentTimeMillis();
        long timeSinceLastDisplay;

        System.out.println("-".repeat(75));

        while (crawler.hasNext() || scraper.hasNext()) {
            timeSinceLastDisplay = System.currentTimeMillis() - lastDisplayTimeEnd;

            // Crawl and scrape the next page
            System.out.println("///    Crawler processed: " + crawler.processNext(scraper, true));
            System.out.println("///    Scraper processed: " + scraper.processNext(crawler));
            System.out.println("///    Crawler visited " + crawler.getNumUrlsVisited() + " out of the " +
                    (crawler.getNumUrlsLeftToVisit() + crawler.getNumUrlsVisited()) + " discovered pages so far. ");
            System.out.println("///    Scraper visited " + scraper.getNumUrlsVisited() + " out of the " +
                    (scraper.getNumUrlsLeftToVisit() + scraper.getNumUrlsVisited()) + " downloaded pages so far. ");
            System.out.println("-".repeat(75));

            // Display results every so often
            if (numProcessedPages % DISPLAY_INTERVAL_ITERATIONS == 0 || timeSinceLastDisplay > DISPLAY_INTERVAL_MS) {
                System.out.print("""
                        "All" - all info
                        "A" - addresses
                        "C" - course codes
                        "D" - (us) dates
                        "E" - emails
                        "EU" - external URLs
                        "I" - image links
                        "IU" - internal URLs
                        "P" - phone numbers
                        "N" - don't display results
                        Type one of the above to see the corresponding info:\s""");
                String input = scanner.nextLine();
                HashMap<Scraper.DataType, FindIterable<org.bson.Document>> data = scraper.getAllData();
                List<Scraper.DataType> dataTypesToDisplay = new ArrayList<>();
                switch (input.toUpperCase()) {
                    case "ALL" -> dataTypesToDisplay.addAll(data.keySet());
                    case "A" -> dataTypesToDisplay.add(Scraper.DataType.Address);
                    case "C" -> dataTypesToDisplay.add(Scraper.DataType.CourseCode);
                    case "D" -> dataTypesToDisplay.add(Scraper.DataType.UsDate);
                    case "E" -> dataTypesToDisplay.add(Scraper.DataType.EmailAddress);
                    case "EU" -> dataTypesToDisplay.add(Scraper.DataType.ExternalUrl);
                    case "I" -> dataTypesToDisplay.add(Scraper.DataType.ImageUrl);
                    case "IU" -> dataTypesToDisplay.add(Scraper.DataType.InternalUrl);
                    case "P" -> dataTypesToDisplay.add(Scraper.DataType.PhoneNumber);
                    case "N" -> System.out.println("-".repeat(DISPLAY_COLUMN_WIDTH));
                    default -> System.out.println("No valid input received. Not displaying any results.");
                }

                HashMap<Scraper.DataType, Integer> numOfDataTypeFound = new HashMap<>();
                for (Scraper.DataType type : dataTypesToDisplay) {
                    if (data.containsKey(type)) {
                        StringBuilder sb = new StringBuilder();
                        int numItems = 0;
                        for (org.bson.Document doc : data.get(type)) {
                            sb.append(String.format("%-" + (DISPLAY_COLUMN_WIDTH - 18) + "." +
                                            (DISPLAY_COLUMN_WIDTH - 18) + "s", doc.get("value")))
                                    .append("|")
                                    .append(String.format("%15s", doc.get("totalInstances")))
                                    .append("\n");
                            numItems++;
                        }
                        numOfDataTypeFound.put(type, numItems);
                        System.out.println("\nFound " + numItems + " unique " + type + "s:");
                        System.out.println("-".repeat(DISPLAY_COLUMN_WIDTH));
                        System.out.println(String.format("%-" + (DISPLAY_COLUMN_WIDTH - 18) + "." +
                                (DISPLAY_COLUMN_WIDTH - 18) + "s", type) + "|" +
                                String.format("%-15s", "\sNumber found"));
                        System.out.println("-".repeat(DISPLAY_COLUMN_WIDTH));
                        System.out.println(sb);
                    }
                }
                System.out.println("-".repeat(DISPLAY_COLUMN_WIDTH));
                numOfDataTypeFound.forEach((type, numItems) -> System.out.println("Found " + numItems + " unique " + type + "s."));
                System.out.println("-".repeat(DISPLAY_COLUMN_WIDTH));
                lastDisplayTimeEnd = System.currentTimeMillis();
            }
            numProcessedPages++;
        }
    }
}