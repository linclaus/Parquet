package com.gridsum.parquet;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

public class GenerateParquetFile { 
 
	static String ClusterName = "nameservice1";
	private static final String HADOOP_URL = "hdfs://"+ClusterName;
//	public static Configuration conf;

//    static {
//        conf = new Configuration();
//        conf.set("fs.defaultFS", HADOOP_URL);
//        conf.set("dfs.nameservices", ClusterName);
//        conf.set("dfs.ha.namenodes."+ClusterName, "nn1,nn2");
//        conf.set("dfs.namenode.rpc-address."+ClusterName+".nn1", "172.16.50.24:8020");
//        conf.set("dfs.namenode.rpc-address."+ClusterName+".nn2", "172.16.50.21:8020");
//        //conf.setBoolean(name, value);
//        conf.set("dfs.client.failover.proxy.provider."+ClusterName, 
//        		"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//    }
    
    public static final MessageType FILE_SCHEMA = Types.buildMessage()
		      .required(PrimitiveTypeName.INT32).named("id")
		      .required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("name")
		      .named("test");
    
    public static void createFile1(Configuration conf) throws Exception {
		String file = "/user/linzhiqiang/test_parquet_file7.parquet";
		Path path = new Path(file);
		FileSystem fs = path.getFileSystem(conf);
	    if (fs.exists(path)) {
	      fs.delete(path, true);
	    }
		GroupWriteSupport.setSchema(FILE_SCHEMA, conf);
	    SimpleGroupFactory f = new SimpleGroupFactory(FILE_SCHEMA);
		@SuppressWarnings("deprecation")
		ParquetWriter<Group> writer = new ParquetWriter<Group>(path, new GroupWriteSupport(),
				CompressionCodecName.SNAPPY, 1024, 1024, 512, true, false, WriterVersion.PARQUET_2_0, conf);
		for (int i = 0; i < 100; i++) {
          writer.write(
              f.newGroup()
              .append("id", i)
              .append("name", UUID.randomUUID().toString()));
        }
        writer.close();
	}
    public static void main(String[] args) throws Exception {
    	Configuration conf=new Configuration();
		createFile1(conf);
	}
 
}