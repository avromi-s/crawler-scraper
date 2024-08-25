// 5.30.2023

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mongodb.client.*;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.conversions.Bson;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import static com.mongodb.client.model.Sorts.*;

/**
 * The scraper processes and extracts all relevant information from the provided web pages (supplied by the crawler).
 */
public class Scraper {

    /**
     * The data types that the scraper collects
     */
    public enum DataType {
        InternalUrl, ExternalUrl, ImageUrl, PhoneNumber, EmailAddress, Address, UsDate, CourseCode, UnknownUrls
    }

    private final MongoCollection<org.bson.Document> dataCollection;  // holds the data scraped from all pages
    private final MongoCollection<org.bson.Document> urlsCollection;  // holds the urls to be visited and the urls already visited
    private final org.bson.Document urlsToVisitFilter = new org.bson.Document().append("type", "toVisit");
    private final org.bson.Document visitedUrlsFilter = new org.bson.Document().append("type", "visited");
    private final String domainRegex;
    private final LinkedList<Document> pagesToVisit;
    private final HashSet<String> pagesVisited;
    // Contains the hashcodes of all page urls that either will be or were visited so that we don't visit any page more than once.
    private final HashSet<Integer> allEncounteredPageUrls;


    /**
     * On instantiation load any previously encountered urls into their respective collections so that we can continue
     * from where we last left off.
     *
     * @param domain         used to differentiate between internal and external urls. internal urls will be given to the crawler
     *                       to download and subsequently handed back to the scraper.
     * @param dataCollection the MongoDB collection of all retrieved data
     * @param urlsCollection the MongoDB collection of the urls left to visit and the urls already visited
     */
    public Scraper(String domain, MongoCollection<org.bson.Document> dataCollection, MongoCollection<org.bson.Document> urlsCollection) {
        this.domainRegex = domain.replace(".", "\\.");
        this.dataCollection = dataCollection;
        this.urlsCollection = urlsCollection;
        this.pagesToVisit = new LinkedList<>();
        this.pagesVisited = new HashSet<>();
        this.allEncounteredPageUrls = new HashSet<>();

        // Load all previously encountered urls from the db
        Bson projection = Projections.fields(Projections.include("urls"), Projections.excludeId());

        // Pages to be visited
        org.bson.Document urlsToVisit = urlsCollection.find(urlsToVisitFilter).projection(projection).first();
        if (urlsToVisit != null) {
            urlsToVisit.getList("urls", String.class).forEach(item -> {
                try {
                    this.pagesToVisit.add(Jsoup.connect(item).get());
                } catch (IOException e) {
                    System.out.println("Scraper error retrieving page from db url - page not queued for scraping. "
                            + e.getMessage());
                }
            });
        }

        // Pages already visited are added to the colored and visited urls collections
        org.bson.Document visitedUrls = urlsCollection.find(visitedUrlsFilter).projection(projection).first();
        if (visitedUrls != null) {
            this.pagesVisited.addAll(visitedUrls.getList("urls", String.class));
            this.pagesVisited.forEach(item -> this.allEncounteredPageUrls.add(item.hashCode()));
        }
    }


    /**
     * Retrieve all the data collected so far from the database.
     *
     * @return A HashMap with all the data, mapping each DataType to an iterable
     */
    public HashMap<DataType, FindIterable<org.bson.Document>> getAllData() {
        HashMap<DataType, FindIterable<org.bson.Document>> values = new HashMap<>();
        for (DataType type : DataType.values()) {
            org.bson.Document filter = new org.bson.Document().append("type", type);
            Bson projection = Projections.fields(Projections.include("type"),
                    Projections.include("value"),
                    Projections.include("totalInstances"),
                    Projections.excludeId());
            Bson orderBySort = orderBy(ascending("type"),
                    descending("totalInstances"),
                    ascending("value"));
            FindIterable<org.bson.Document> items = dataCollection.find(filter).projection(projection).sort(orderBySort);
            values.put(type, items);
        }
        return values;
    }

    /**
     * Add a page to be scraped to the collections in memory and the db.
     *
     * @param htmlPage the page to add
     * @return <code>true</code> if the operation was successful;
     * <code>false</code> otherwise.
     */
    public boolean queuePage(Document htmlPage) {
        if (!allEncounteredPageUrls.contains(htmlPage.hashCode())) {
            // Add the url to the 'toVisit' document in the db
            allEncounteredPageUrls.add(htmlPage.hashCode());

            // In the db, store the url instead of the actual page to save space and so that we can reload the page
            // more recently whenever it's actually evaluated
            Bson updates = Updates.push("urls", htmlPage.location());
            UpdateOptions options = new UpdateOptions().upsert(true);
            urlsCollection.updateOne(urlsToVisitFilter, updates, options);
            return pagesToVisit.add(htmlPage);
        }
        return false;
    }

    /**
     * Process the next page: Extract all info, store it in the db, and queue any newly found internalUrls into the crawler.
     *
     * @param crawlerToQueueUrlsTo the crawler that the internal urls found should be added to
     * @return the url of the page processed if it is processed successfully; otherwise null.
     */
    public String processNext(Crawler crawlerToQueueUrlsTo) {
        // Get the next page and set it as visited in memory and in the db
        Document page = pagesToVisit.poll();
        if (page != null) {
            Bson updates = Updates.pull("urls", page.location());
            UpdateOptions options = new UpdateOptions().upsert(false);
            urlsCollection.updateOne(urlsToVisitFilter, updates, options);

            pagesVisited.add(page.location());
            updates = Updates.combine(Updates.push("urls", page.location()));
            options = new UpdateOptions().upsert(true);
            urlsCollection.updateOne(visitedUrlsFilter, updates, options);

            HashMap<DataType, HashMap<String, Integer>> pageData = extractPageData(page);
            if (!pageData.isEmpty()) {
                // Get all internal urls and queue them into the crawler
                if (pageData.containsKey(DataType.InternalUrl)) {
                    pageData.get(DataType.InternalUrl).keySet().stream().filter(Objects::nonNull)
                            .forEach(crawlerToQueueUrlsTo::queueUrl);
                }

                // Store results in the db, inserting or updating a document for each value found.
                for (DataType type : pageData.keySet()) {
                    for (String item : pageData.get(type).keySet()) {
                        org.bson.Document filter = new org.bson.Document().append("type", type).append("value", item);
                        Bson projection = Projections.fields(Projections.include("totalInstances"), Projections.excludeId());
                        org.bson.Document doc = dataCollection.find(filter).projection(projection).first();
                        int previousTotalInstances = doc != null ? (int) doc.get("totalInstances") : 0;
                        org.bson.Document update = new org.bson.Document().append("url", page.location()).append("count", pageData.get(type).get(item));
                        updates = Updates.combine(Updates.push("numInstancesByUrl", update), Updates.set("totalInstances", previousTotalInstances + pageData.get(type).get(item)));
                        options = new UpdateOptions().upsert(true);
                        dataCollection.updateOne(filter, updates, options);
                    }
                }
                return page.location();
            }
        }
        return null;
    }

    /**
     * Extract all data types from the page.
     *
     * @param page the page to extract from
     * @return a 2D HashMap mapping each DataType to a HashMap that maps the data item to the number of times it was
     * found on the page.
     */
    private HashMap<DataType, HashMap<String, Integer>> extractPageData(Document page) {
        HashMap<DataType, HashMap<String, Integer>> pageData = new HashMap<>();
        if (page != null) {
            // Address:
            // group 1 - house number,
            // group 2 - street,
            // group 3 - city,
            // group 4 - state (code or name),
            // group 5 - 5 digit zip code,
            // group 6 - 4 digit zip code suffix, if applicable
            // "(?i)(\\w+(?:-\\w+)?) ((?:\\d*[a-z]+\\.? )*[a-z]+\\.?)\\s((?:[a-z]+ )*[a-z]+), ([A-Z]{2}) (\\d{5})(?:-(\\d{4}))?";
            String addressPattern = "(?i)(\\w+(?:-\\w+)?) ((?:\\d*[a-z]+\\.? )*[a-z]+\\.?)\\s((?:[a-z]+ )*[a-z]+), " +
                    "([A-Z]{2}|[a-z]+(?: [a-z]+)) (\\d{5})(?:-(\\d{4}))?";
            pageData.put(DataType.Address, extractItemsMatchingPattern(page, addressPattern));

            // UsDate:
            // group 1 - Month,
            // group 2 - day,
            // group 3 - year
            String usDatePattern = "((?i)JAN(?:\\.|UARY)|FEB(?:\\.|RUARY)|MAR(?:\\.|CH)|APR(?:\\.|IL)|MAY|JUNE|JULY|" +
                    "AUG(?:\\.|UST)|SEPT(?:\\.|EMBER)|OCT(?:\\.|OBER)|NOV(?:\\.|EMBER)|DEC(?:\\.|EMBER)|0?[1-9]|1[0-2])" +
                    "(?:\\s|\\s?[/\\.]\\s?)(0?[1-9]|[1-2][1-9]|3[0-1])(?:\\s|\\s?[/\\.,]\\s?)(\\d{4})";
            pageData.put(DataType.UsDate, extractItemsMatchingPattern(page, usDatePattern));


            String courseCodePattern = "(?<!\\w)[A-Z]{4}\\s\\d{3}(?!\\w)";
            pageData.put(DataType.CourseCode, extractItemsMatchingPattern(page, courseCodePattern));

            // Extract all url data types and add it to the pageData HashMap.
            mergeAndIncrementDuplicatesInMaps(pageData, extractAllUrlTypes(page));
        }
        return pageData;
    }


    /**
     * Extract the different types of urls found on the page. Includes internal, external, phone number, email, and image urls.
     *
     * @param page the page to extract the urls from
     * @return a 2D HashMap mapping each DataType to a HashMap that maps the data to the number of times it was
     * found on the page.
     */
    private HashMap<DataType, HashMap<String, Integer>> extractAllUrlTypes(Document page) {
        HashMap<DataType, HashMap<String, Integer>> pageData = new HashMap<>();

        pageData.put(DataType.ImageUrl, extractImageUrls(page));

        // Get all links contained in <a> elements as full (absolute) urls without their anchors (the part after #)
        Elements links = page.select("a[href]");
        HashMap<String, Integer> absoluteUrls = new HashMap<>();
        links.forEach(url -> {
            String absoluteUrl = url.absUrl("href");
            String pageUrlWithoutAnchor = absoluteUrl.contains("#") ? absoluteUrl.substring(0, absoluteUrl.indexOf("#")) : absoluteUrl;
            if (!absoluteUrls.containsKey(pageUrlWithoutAnchor)) {
                absoluteUrls.put(pageUrlWithoutAnchor, 1);
            } else {
                absoluteUrls.put(pageUrlWithoutAnchor, absoluteUrls.get(pageUrlWithoutAnchor) + 1);
            }
        });

        HashMap<String, Integer> internalUrls = new HashMap<>();
        HashMap<String, Integer> externalUrls = new HashMap<>();
        HashMap<String, Integer> emailUrls = new HashMap<>();
        HashMap<String, Integer> phoneNumberUrls = new HashMap<>();
        HashMap<String, Integer> unknownUrls = new HashMap<>();

        Pattern internalUrl = Pattern.compile("https?://((\\w|\\d)+\\.)*" + domainRegex + ".*");
        Pattern externalUrl = Pattern.compile("https?://(?!" + domainRegex + ")+(\\w|\\d)+\\.(?!" + domainRegex + ").*");
        Pattern email = Pattern.compile("^(mailto:)(.*)");
        Pattern phoneNumber = Pattern.compile("^(tel:)(.*)");

        // Collect each link as the appropriate DataType
        for (String url : absoluteUrls.keySet()) {
            Matcher internalUrlMatcher = internalUrl.matcher(url);
            Matcher externalUrlMatcher = externalUrl.matcher(url);
            Matcher emailUrlMatcher = email.matcher(url);
            Matcher phoneNumberUrlMatcher = phoneNumber.matcher(url);

            int numOccurrencesOnPage = absoluteUrls.get(url);
            if (internalUrlMatcher.matches()) {
                internalUrls.put(url, numOccurrencesOnPage);
            } else if (externalUrlMatcher.matches()) {
                externalUrls.put(url, numOccurrencesOnPage);
            } else if (emailUrlMatcher.matches()) {
                emailUrls.put(url, numOccurrencesOnPage);
            } else if (phoneNumberUrlMatcher.matches()) {
                phoneNumberUrls.put(url, numOccurrencesOnPage);
            } else {
                unknownUrls.put(url, numOccurrencesOnPage);
            }
        }

        // Pack and return
        pageData.put(DataType.InternalUrl, internalUrls);
        pageData.put(DataType.ExternalUrl, externalUrls);
        pageData.put(DataType.EmailAddress, emailUrls);
        pageData.put(DataType.PhoneNumber, phoneNumberUrls);
        pageData.put(DataType.UnknownUrls, unknownUrls);
        return pageData;
    }

    /**
     * Extract the image urls found on the page.
     *
     * @param page the page to extract from
     * @return a HashMap mapping each image url found to the number of times it was found
     */
    private HashMap<String, Integer> extractImageUrls(Document page) {
        Elements images = page.select("img[src]");
        HashMap<String, Integer> imageLinks = new HashMap<>();
        images.forEach(link -> {
            String absoluteLink = link.absUrl("src");
            if (!imageLinks.containsKey(absoluteLink)) {
                imageLinks.put(absoluteLink, 1);
            } else {
                imageLinks.put(absoluteLink, imageLinks.get(absoluteLink) + 1);
            }
        });
        return imageLinks;
    }

    /**
     * Extract all items on the page that match the given regex pattern.
     *
     * @param doc     the page to extract from
     * @param pattern the pattern to extract matching text
     * @return a HashMap mapping each value found to the number of times it was found on the page.
     */
    private HashMap<String, Integer> extractItemsMatchingPattern(Document doc, String pattern) {
        HashMap<String, Integer> data = new HashMap<>();
        Pattern itemPattern = Pattern.compile(pattern);

        // For each match found, add it to the HashMap, incrementing the int value if it was already there.
        doc.select("*:matchesOwn(" + pattern + ")").forEach(x -> {
            Matcher matcher = itemPattern.matcher(x.text());
            // matcher.find() must be called before calling .group() so that .group() gets results.
            if (matcher.find()) {
                String thisMatch = matcher.group();
                data.put(thisMatch, data.containsKey(thisMatch) ? data.get(thisMatch) + 1 : 1);
            }
        });
        return data;
    }

    /**
     * Merge 2 - 2D HashMaps into one, joining the inner maps by incrementing their (integer) values for
     * each new duplicate caused by the merge. The inner map is some Data type mapped to an integer that counts the
     * number of times the key occurred.
     *
     * @param originalMap the original map to merge into
     * @param mapToAdd    the map to add the items from
     */
    private <T, U> void mergeAndIncrementDuplicatesInMaps(HashMap<T, HashMap<U, Integer>> originalMap, HashMap<T, HashMap<U, Integer>> mapToAdd) {
        mapToAdd.forEach((k, v) -> originalMap.merge(k, v, (originalValues, mapToAddValues) -> {
            HashMap<U, Integer> originalCopy = new HashMap<>(originalValues);
            HashMap<U, Integer> mapToAddCopy = new HashMap<>(mapToAddValues);
            mergeAndIncrementDuplicates(originalCopy, mapToAddCopy);
            return originalCopy;
        }));
    }

    /**
     * Merge 2 maps, incrementing the Integer value for each new duplicate caused by the merge.
     *
     * @param original the original map to merge into
     * @param add      the map to add the items from
     */
    private <U> void mergeAndIncrementDuplicates(HashMap<U, Integer> original, HashMap<U, Integer> add) {
        add.forEach((k, v) -> original.merge(k, v, Integer::sum));
    }

    /**
     * Indicate if the scraper currently has at least one more page left to visit.
     *
     * @return <code>true</code> if the scraper currently has at least one more page left to visit; otherwise <code>false</code>.
     */
    public boolean hasNext() {
        return !pagesToVisit.isEmpty();
    }

    /**
     * Get the number of urls left to visit
     *
     * @return the number of urls left to visit
     */
    public int getNumUrlsLeftToVisit() {
        return pagesToVisit.size();
    }

    /**
     * Get the number of urls visited
     *
     * @return the number of urls visited
     */
    public int getNumUrlsVisited() {
        return pagesVisited.size();
    }
}

