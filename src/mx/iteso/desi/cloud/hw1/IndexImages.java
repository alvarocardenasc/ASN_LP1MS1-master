package mx.iteso.desi.cloud.hw1;

import java.util.HashSet;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import mx.iteso.desi.cloud.keyvalue.IKeyValueStorage;
import mx.iteso.desi.cloud.keyvalue.KeyValueStoreFactory;
import mx.iteso.desi.cloud.keyvalue.ParseTriples;
import mx.iteso.desi.cloud.keyvalue.PorterStemmer;
import mx.iteso.desi.cloud.keyvalue.Triple;

public class IndexImages {

    ParseTriples parser;
    IKeyValueStorage imageStore, titleStore;
    private final int MAX_PAIRINGS = 254;
    private final int MAX_ENTRIES = 10000;
    private int putting=0;

    public IndexImages(IKeyValueStorage imageStore, IKeyValueStorage titleStore) {
        this.imageStore = imageStore;
        this.titleStore = titleStore;
    }

    public void run(String imageFileName, String titleFileName) throws IOException {
        try {
            System.out.println("TODO: This method should load all images and titles into the two key-value stores.");
            System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            System.out.println("imageFileName=" + imageFileName + " titleFileName" + titleFileName);

            imageStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, "images");
            titleStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, "terms");

            indexImageFile(Config.pathToFiles + Config.imageFileName);
            indexTermsFile(Config.pathToFiles + Config.titleFileName);
        } catch (Exception e) {
            System.err.println("---------" + e);
        } finally {
            imageStore.close();
            titleStore.close();
        }
        System.out.println("---------------------------------------------------");
        //TODO: close the databases;
    }

    private void indexImageFile(String fileName) throws IOException {
        ParseTriples imageParser = new ParseTriples(fileName);
        Triple triple = imageParser.getNextTriple();
        String filter = Config.filter.toLowerCase();

        int counter = 0;
        // Parse through triples 
        while (triple != null && counter < MAX_ENTRIES) {
            counter++;
            // Regularize data
            String categoryURL = triple.getSubject().toLowerCase();
            String relationship = triple.getPredicate().toLowerCase();
            String imgURL = triple.getObject();
            // Get topic from substring following the last '/'
            int lastSlashIdx = categoryURL.lastIndexOf("/");
            String topic = categoryURL.substring(lastSlashIdx + 1,
                    categoryURL.length()).toLowerCase();
            // Check if relationship is valid
            if (relationship.equals("http://xmlns.com/foaf/0.1/depiction")) {
                // Enter into store if no filter was entered or if topic begins
                // with the filter
                if (filter.equals("") || topic.matches("^" + filter + "+.*$")) {
                    imageStore.put(categoryURL, imgURL);
                     System.out.println(putting++ +" Put: "+categoryURL+" "+imgURL);       
                }
            }
            triple = imageParser.getNextTriple();
        }
        imageParser.close();
    }

    private void indexTermsFile(String fileName) throws IOException {
        ParseTriples termParser = new ParseTriples(fileName);
        Triple triple = termParser.getNextTriple();

        int counter = 0;
        // Parse through triples
        while (triple != null && counter < MAX_ENTRIES) {
            counter++;
            String categoryURL = triple.getSubject().toLowerCase();
            // Check if category is in image database
            if (imageStore.exists(categoryURL)) {
                String label = triple.getObject();
                // Check for cases like 'ASCII' or 'AbalonE'
                if (label.matches("^[A-Z]+$")
                        || label.matches("^[A-Z][a-z]+[A-Z]$")) {
                    // Check for max pairings
                    if (titleStore.get(PorterStemmer.stem(label.toLowerCase())).size() < MAX_PAIRINGS) {
                        titleStore.put(PorterStemmer.stem(label.toLowerCase()), categoryURL);
                        triple = termParser.getNextTriple();
                        continue;
                    }
                }
                // For any other case, pattern match to split up into terms
                Pattern p = Pattern.compile("(\\W*[A-Z0-9]*[a-z]*)");
                Matcher m = p.matcher(label);
                // Iterate over matches found
                while (m.find()) {
                    // Get rid of any non-word characters, ready for stemming
                    String subsequence = m.group(1).replaceAll("\\W", "")
                            .toLowerCase();
                    // Only store if string is not empty
                    if (!subsequence.isEmpty() && !PorterStemmer.stem(subsequence).isEmpty()) {
                        // Store each term key with the corresponding category, check for max pairings
                        if (titleStore.get(PorterStemmer.stem(subsequence)).size() < MAX_PAIRINGS) {
                            titleStore.put(PorterStemmer.stem(subsequence), categoryURL);
                        }
                    }
                }
            }
            triple = termParser.getNextTriple();
        }
        termParser.close();
    }

    public static void main(String args[]) {
        // TODO: Add your own name here
        System.out.println("35*** Alumno: _Hugo____________________ (Exp: __Ms705203_______ )");
        try {

            IKeyValueStorage imageStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType,
                    "images");
            IKeyValueStorage titleStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType,
                    "terms");

            IndexImages indexer = new IndexImages(imageStore, titleStore);
            indexer.run(Config.imageFileName, Config.titleFileName);
            System.out.println("Indexing completed");

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to complete the indexing pass -- exiting");
        }
    }
}
