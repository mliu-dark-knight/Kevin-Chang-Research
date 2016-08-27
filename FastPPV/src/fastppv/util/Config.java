package fastppv.util;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.util.Properties;

public final class Config {

    public static int    numIterations				= 30;
    public static int    numHubs					= 3000;
    public static double epsilon                    = 1E-8;
    public static double clip                       = 1E-4;
    public static double delta		                = 1E-4;
    //public static int	 numHubsPerIter				= 10;
    public static int    eta		                = 2;
    public static double alpha						= 0.15;
    public static int	 numClusters				= 3;
    public static int	 maxClusterFaults			= 10;
    public static int	 numRepetitions				= 5;

    public static String prefix                     = "/Users/mengxiongliu/Desktop/ECE_CS/Kevin_Chang/FastPPV/dblp/";
    public static String inputprefix                 = "/Users/mengxiongliu/Desktop/ECE_CS/Kevin_Chang/";
    public static String nodeFile					= inputprefix + "karate.node";
    public static String edgeFile					= inputprefix + "karate.edgelist";
    public static String indexDir				    = prefix;
    public static String clusterDir				    = "";
    public static String communityFile				= "";
    public static String outputDir		            = prefix + "vectors";
    public static String queryFile      		    = prefix + "queries";
    public static String hubType                    = "";    
    
    public static int resultTop						= 1000;
    public static int hubTop						= 100000;
    public static int iterations                    = 10;
    public static int progressiveTopK				= 100;
    

    private static boolean hasValidProp(Properties prop, Field field) {
    	return prop.getProperty(field.getName()) != null
        	&& !prop.getProperty(field.getName()).trim().isEmpty();
    }
    
    private static String getProp(Properties prop, Field field) {
    	return prop.getProperty(field.getName()).trim();
    }

    private static void setInt(Properties prop, Field field) throws Exception {
        if (hasValidProp(prop, field))
            field.set(null, Integer.valueOf(getProp(prop, field)));
    }

    private static void setDouble(Properties prop, Field field) throws Exception {
        if (hasValidProp(prop, field)) {
            field.set(null, Double.valueOf(getProp(prop, field)));
        }
    }

    private static void setString(Properties prop, Field field) throws Exception {
        if (hasValidProp(prop, field)) {
            field.set(null, getProp(prop, field));
        }
    }
    
    public static void print() {
    	try {
    		for (Field field : Config.class.getFields())
    			System.out.println(field.getName() + " = " + field.get(null));
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

}
