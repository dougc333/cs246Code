package cs246workinggroup.hw11a;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.TestParallelReadUtil;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;

public class Test {

	public static void main(String []args) throws Exception{
		//test configuration
		Configuration conf = new Configuration();
		HdfsConfiguration hc = new HdfsConfiguration();
		
		DFSClient client = new DFSClient(new URI("hdfs://localhost:8020"),conf);
		//print out short circuit configuration
		System.out.println("ClientName: "+client.getClientName());
		
		//when we do a read we should be able to figure out if this is a remote block 
		//or a local block and verify this is a local read vs. a remote read
		//Returns null if SCR setup correctly as in CDH
		//true?
		//conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_KEY, true);
		System.out.println("DomainSocket Load: "+DomainSocket.getLoadingFailureReason());
//		TemporarySocketDirectory sockDir=new TemporarySocketDirectory();
		DomainSocket.disableBindPathValidation(); //what does this do? 
		
		//we should bind to a port then write
		
		//we should see a sockDir.close()
		
		//conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY, new File(sockDir.getDir(),"Test"));
		conf.setLong("dfs.blocksize", 1048576); //smaller block size so we have more blocks spread out. Big ones take too long
		conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY, true);
		FileSystem fs = FileSystem.get(new URI("hdfs://localhost:8020"),conf);
		
		Path TEST_PATH=new Path("/tmp/test1.txt");
		if(fs.exists(TEST_PATH)){
			fs.delete(TEST_PATH);
		}
		//should make this a multiple of block size, ie. 10 blocks
		int TEST_LENGTH=1048576*5; //this creates a file of this length
		int BUFF_LEN=4096;
		long BLOCK_SIZE=1048576;
		long startWriteTime=System.currentTimeMillis();
		// createFile(FileSystem fs, Path fileName, int bufferLen,
//        long fileLen, long blockSize, short replFactor, long seed)

		DFSTestUtil.createFile(fs, TEST_PATH,BUFF_LEN, TEST_LENGTH,BLOCK_SIZE, (short)1, 5678L);
		long endWriteTime = System.currentTimeMillis();
		System.out.println("WriteTime:"+(endWriteTime-startWriteTime));
		FSDataInputStream dis = fs.open(TEST_PATH);
		byte buf[] = new byte[TEST_LENGTH];
		long startReadTime= System.currentTimeMillis();
	    IOUtils.readFully(dis, buf, 0, TEST_LENGTH);
	    long endReadTime = System.currentTimeMillis();
	    dis.close();
	    System.out.println("read time:"+(endReadTime-startReadTime));

		//we dont know if the above are local reads, remote reads and what combo they are
	    System.out.println("DefaultBlockSize: "+client.getDefaultBlockSize());
		System.out.println("Replicaiton, this isn't same as 1 set in .createFile: "+client.getDefaultReplication()); //careful
		ClientProtocol cp = client.getNamenode();
		System.out.println("TEST_PATH.getName():"+TEST_PATH.getName());
				
		LocatedBlocks lb = cp.getBlockLocations("/tmp/test1.txt", 0, TEST_LENGTH);
		System.out.println("block count:"+lb.locatedBlockCount());
		for(int i=0;i<lb.locatedBlockCount();i++){
			LocatedBlock lblock = lb.get(i);
			System.out.println("block size:"+lblock.getBlockSize());
			System.out.println("block startOffset:"+lblock.getStartOffset());
			DatanodeInfo dninfo[] = lblock.getLocations();
			for(int j=0;j<dninfo.length;j++){
				System.out.println(dninfo[j].dumpDatanode());
			}
		}
	    
	    	    
	    
	    
		//make sure you call BlockReaderLocal for SC reads
		
		
		//do we need to modify this to verify it shows up in the logs? 
		
		
		//test write, read from this FileSystem
		//fs.getFileBlockLocations(file, start, len); //egad this is easier than above
		
		
	}
}
