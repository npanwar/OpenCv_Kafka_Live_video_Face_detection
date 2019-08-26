package sparkStream;

import java.io.File;
import java.io.InputStream;

import org.opencv.core.Core;
public class LoadLibraries {

	private static void loadLibraries() {

	    try {
	        InputStream in = null;
	        File fileOut = null;
	        String osName = System.getProperty("os.name");
	        String opencvpath = System.getProperty("user.dir");
	        //if(osName.startsWith("Windows")) {
	            int bitness = Integer.parseInt(System.getProperty("sun.arch.data.model"));
	            if(bitness == 32) {
	                opencvpath=opencvpath+"\\opencv\\x86\\";
	            }
	            else if (bitness == 64) { 
	                opencvpath=opencvpath+"\\opencv\\x64\\";
	            } else { 
	                opencvpath=opencvpath+"\\opencv\\x86\\"; 
	            }           
			/*
			 * } else if(osName.equals("Mac OS X")){ opencvpath =
			 * opencvpath+"Your path to .dylib"; }
			 */  opencvpath ="C:\\opencv\\build\\java\\x64\\";
	        System.out.println(opencvpath);
	        System.load(opencvpath + Core.NATIVE_LIBRARY_NAME + ".dll");
	    } catch (Exception e) {
	        throw new RuntimeException("Failed to load opencv native library", e);
	    }
	}
	
	
	public static void main(String[] args) {
	    loadLibraries();
	} 
	
	
}
