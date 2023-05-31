// Avromi Schneierson - 5.30.2023

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.conversions.Bson;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import java.io.IOException;
import java.util.*;

/**
 * The crawler is responsible for downloading the pages for all internal urls found. These are provided by the scraper
 * when they are found.
 */
public class Crawler {
    private final MongoCollection<org.bson.Document> urlsCollection;  // holds the urls to be visited and the urls already visited
    private final org.bson.Document urlsToVisitFilter = new org.bson.Document().append("type", "toVisit");
    private final org.bson.Document urlsVisitedFilter = new org.bson.Document().append("type", "visited");
    private final LinkedList<String> urlsToVisit;

    private final HashSet<String> urlsVisited;
    // Contains the hashcodes of all urls that either will be or were visited so that we don't visit any url more than once.
    private final HashSet<Integer> allEncounteredUrls;
    private long lastDownloadEndTime;
    private long lastDownloadDuration = 0;
    private final long DEFAULT_DOWNLOAD_INTERVAL_MS = 10000;
    private String currentUrl;

    /**
     * On instantiation load any previously encountered urls into their respective collections so that we can continue
     * from where we last left off.
     *
     * @param initialUrl     the url to start crawling from
     * @param urlsCollection the MongoDB collection holding the urls previously encountered
     */
    public Crawler(String initialUrl, MongoCollection<org.bson.Document> urlsCollection) {
        this.lastDownloadEndTime = System.currentTimeMillis() - DEFAULT_DOWNLOAD_INTERVAL_MS;  // so that the first page is processed immediately
        this.urlsCollection = urlsCollection;
        this.urlsToVisit = new LinkedList<>();
        this.urlsVisited = new HashSet<>();
        this.allEncounteredUrls = new HashSet<>();


        // Add all previously encountered urls from the db to their respective collections
        Bson projection = Projections.fields(Projections.include("urls"), Projections.excludeId());

        // Pages to be visited
        org.bson.Document urlsToVisit = urlsCollection.find(urlsToVisitFilter).projection(projection).first();
        if (urlsToVisit != null) {
            this.urlsToVisit.addAll(urlsToVisit.getList("urls", String.class));
        }

        // Pages already visited
        org.bson.Document urlsVisited = urlsCollection.find(urlsVisitedFilter).projection(projection).first();
        if (urlsVisited != null) {
            this.urlsVisited.addAll(urlsVisited.getList("urls", String.class));
            this.urlsVisited.forEach(item -> this.allEncounteredUrls.add(item.hashCode()));
        }

        queueUrl(initialUrl);
    }

    /**
     * Add an url to be crawled (downloaded) to the collections in memory and the db. The url is only added if it has not
     * been previously encountered.
     *
     * @param url the url to add
     * @return <code>true</code> if the url was added successfully;
     * <code>false</code> otherwise.
     */
    public boolean queueUrl(String url) {
        boolean issueOccurredAddingAUrl = false;
        if (!allEncounteredUrls.contains(url.hashCode())) {
            // Add the url to the 'toVisit' document in db
            Bson updates = Updates.push("urls", url);
            UpdateOptions options = new UpdateOptions().upsert(true);
            boolean dbUpdated = urlsCollection.updateOne(urlsToVisitFilter, updates, options).getModifiedCount() > 0;

            boolean urlQueuedForVisiting = urlsToVisit.add(url);
            boolean urlColored = allEncounteredUrls.add(url.hashCode());
            issueOccurredAddingAUrl = dbUpdated && urlQueuedForVisiting && urlColored;
        }
        return issueOccurredAddingAUrl;
    }

    /**
     * Process the next page: download the next url on the queue and queue it into the scraper provided.
     *
     * @param scraperToQueuePagesTo the scraper to queue the urls into
     * @return the url of the page processed if it is processed successfully; otherwise null.
     */
    public String processNext(Scraper scraperToQueuePagesTo, boolean displayDownloadLoadingMessage) {
        // Get the next page and add it to the scraper, removing it from the urlsToVisit queue and db document
        currentUrl = urlsToVisit.poll();

        if (currentUrl != null) {
            Bson update = Updates.pull("urls", currentUrl);
            UpdateOptions options = new UpdateOptions().upsert(false);  // if it doesn't already exist then no need to create a new document
            urlsCollection.updateOne(urlsToVisitFilter, update, options);

            urlsVisited.add(currentUrl);
            update = Updates.push("urls", currentUrl);
            options = new UpdateOptions().upsert(true);  // here, we want to create a new document if it doesn't already exist
            urlsCollection.updateOne(urlsVisitedFilter, update, options);

            // Download the page and queue it into the scraper, waiting if not enough time has elapsed since the last download.
            try {
                long elapsedTimeSinceLastDownload = System.currentTimeMillis() - lastDownloadEndTime;
                long timeToWait = Long.max(lastDownloadDuration * 2, DEFAULT_DOWNLOAD_INTERVAL_MS) - elapsedTimeSinceLastDownload;
                if (timeToWait > 0) {
                    if (displayDownloadLoadingMessage) {
                        System.out.println("Crawler is waiting " + (timeToWait / 1000) + " seconds before downloading the next page...");
                    }
                    Thread.sleep(timeToWait);
                }

                long startTime = System.currentTimeMillis();
                Document page = Jsoup.connect(currentUrl).get();
                lastDownloadEndTime = System.currentTimeMillis();
                lastDownloadDuration = lastDownloadEndTime - startTime;

                if (scraperToQueuePagesTo.queuePage(page)) {
                    return currentUrl;
                }
            } catch (IOException | InterruptedException e) {
                // Note that in the case of an inaccessible page or invalid url, the crawler will count the page as visited,
                // (because it was already removed from the memory collections and db). But since it can't actually download
                // it, it will not be given to the scraper.
                return null;
            }
        }
        return null;
    }

    /**
     * Indicate if the crawler currently has at least one more page left to visit.
     *
     * @return <code>true</code> if the crawler currently has at least one more page left to visit; otherwise <code>false</code>.
     */
    public boolean hasNext() {
        return !urlsToVisit.isEmpty();
    }

    /**
     * Get the number of urls left to visit
     *
     * @return the number of urls left to visit
     */
    public int getNumUrlsLeftToVisit() {
        return urlsToVisit.size();
    }

    /**
     * Get the number of urls visited
     *
     * @return the number of urls visited
     */
    public int getNumUrlsVisited() {
        return urlsVisited.size();
    }

    /**
     * Get the url that is currently being, or about to be processed.
     *
     * @return the url currently, or about to be processed.
     */
    public String getCurrentUrl() {
        return currentUrl;
    }
}
