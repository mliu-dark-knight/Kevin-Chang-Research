package fastppv.util;

import java.io.File;

public class IndexManager {

    public static final String FILE_SEP = System.getProperties().getProperty("file.separator");
    
    public static String getIndexShallowDir() {
    	String path = Config.indexDir;
    	if (!path.endsWith(FILE_SEP))
    		path += FILE_SEP;
    	
    	path += Config.hubType + FILE_SEP;
    	return path;
    }
    
    public static String getIndexPPVCountInfoFilename(){
    	return getIndexShallowDir()+Config.numHubs+"countInfo";
    }
    
    public static String getIndexDeepDir() {
    	return getIndexShallowDir() + Config.numHubs + FILE_SEP;
    }
    
    public static String getHubNodeFile() {
    	return getIndexShallowDir() + "hubs";
    }

    public static String getPrimePPVFilename(int nodeId) {
    	return getIndexDeepDir() + nodeId;
    }
    
    public static void mkSubDirShallow() {
        File f = new File(getIndexShallowDir());
        if (!f.exists())
        	f.mkdirs();
    }

    public static void mkSubDirDeep() {
        File f = new File(getIndexDeepDir());
        if (!f.exists())
        	f.mkdirs();
    }

}
