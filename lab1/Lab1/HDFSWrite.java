import java.io.*;
import java.lang.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

public class HDFSWrite {

	public static void main(String[] args) throws Exception {

		// The system configuration
		Configuration conf = new Configuration();

		// Get an instance of the Filesystem
		FileSystem fs = FileSystem.get(conf);

		Path input_path = new Path("/class/s14419x/Lab1/bigdata");
		Path output_path = new Path("/scr/yuz1988/Lab1/output.txt");

		// The Input Data Stream to read
		FSDataInputStream filein = fs.open(input_path);

		// The Output Data Stream to write
		FSDataOutputStream fileout = fs.create(output_path);

		// Skip first 5000000000 bytes
		long skipnum = 5000000000L;	
		filein.skip(skipnum);		
		
		// The checksum result, read the first byte
		int result = filein.read();
		
		// Read each byte and do the XOR
		for (int i = 1; i < 1000; i++) {
			int temp = filein.read();		
			// do the XOR
			result = result ^ temp;
		}
		

		// Write the checksum
		fileout.writeChars(byteToBit((byte)result));

		// Close the file and the file system instance
		filein.close();
		fileout.close();
		fs.close();

	}
	
	public static String byteToBit(byte b) {
		String str = "";
		for (int i=7; i>=0; i--) {		
			str = str + (byte)((b>>i) & 0x1);
		}
		return str;
	}

}