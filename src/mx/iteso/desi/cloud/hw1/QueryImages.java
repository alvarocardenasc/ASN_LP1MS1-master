package mx.iteso.desi.cloud.hw1;

import java.util.Set;
import java.util.Iterator;
import java.util.HashSet;
import mx.iteso.desi.cloud.keyvalue.KeyValueStoreFactory;
import mx.iteso.desi.cloud.keyvalue.IKeyValueStorage;
import mx.iteso.desi.cloud.keyvalue.PorterStemmer;

public class QueryImages {

    IKeyValueStorage imageStore;
    IKeyValueStorage titleStore;

    public QueryImages(IKeyValueStorage imageStore, IKeyValueStorage titleStore) {
        this.imageStore = imageStore;
        this.titleStore = titleStore;
    }

    public Set<String> query(String word) {
        // TODO: Return the set of URLs that match the given word,
        //       or an empty set if there are no matches
	HashSet<String> imgURLs = new HashSet<String>();
	// Parse over all categories returned by searching for a term
	for(String category : titleStore.get(PorterStemmer.stem(word.toLowerCase())))
	{
		// For each category, add to the return set all image URLs that match
		for(String imgURL : imageStore.get(category))
		{
			imgURLs.add(imgURL);
		}
	}
    return imgURLs;
  }
    public void close() {
        // TODO: Close the databases
        imageStore.close();
	titleStore.close();
    }

    public static void main(String args[]) {
        String [] args2={"aruba","Archipelago","Aristotle"};
        args=args2;
        try {
            // TODO: Add your own name here
            System.out.println("*** Alumno: ____________hugo_________ (Exp: __ms705203_______ )");

            // TODO: get KeyValueStores
            IKeyValueStorage imageStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType,
                    "images");
            IKeyValueStorage titleStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType,
                    "terms");

            QueryImages myQuery = new QueryImages(imageStore, titleStore);

            for (int i = 0; i < args.length; i++) {
                System.out.println(args[i] + ":");
                Set<String> result = myQuery.query(args[i]);
                Iterator<String> iter = result.iterator();
                while (iter.hasNext()) {
                    System.out.println("  - " + iter.next());
                }
            }

            myQuery.close();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed qeryimages -- exiting");
        }
    }
}
