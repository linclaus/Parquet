package com.gridsum.parquet;


import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HDFSTest01 {
	
	/**
	 * 文件上传
	 * @param src 
	 * @param dst
	 * @param conf
	 * @return
	 */
	public static boolean put2HSFS(String src , String dst , Configuration conf){
		Path dstPath = new Path(dst) ;
		try{
			FileSystem hdfs = dstPath.getFileSystem(conf) ;
//			FileSystem hdfs = FileSystem.get( URI.create(dst), conf) ;
			hdfs.copyFromLocalFile(false, new Path(src), dstPath) ;
		}catch(IOException ie){
			ie.printStackTrace() ;
			return false ;
		}
		return true ;
	}
	
	/**
	 * 文件下载
	 * @param src
	 * @param dst
	 * @param conf
	 * @return
	 */
	public static boolean getFromHDFS(String src , String dst , Configuration conf){
		Path dstPath = new Path(dst) ;
		try{
			FileSystem dhfs = dstPath.getFileSystem(conf) ;
			dhfs.copyToLocalFile(false, new Path(src), dstPath) ;
		}catch(IOException ie){
			ie.printStackTrace() ;
			return false ;
		}
		return true ;
	}
	
	/**
	 * 文件检测并删除
	 * @param path
	 * @param conf
	 * @return
	 */
	public static boolean checkAndDel(final String path , Configuration conf){
		Path dstPath = new Path(path) ;
		try{
			FileSystem dhfs = dstPath.getFileSystem(conf) ;
			if(dhfs.exists(dstPath)){
				dhfs.delete(dstPath, true) ;
			}else{
				return false ;
			}
		}catch(IOException ie ){
			ie.printStackTrace() ;
			return false ;
		}
		return true ;
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		String src = "hdfs://xcloud:9000/user/xcloud/input/core-site.xml" ;
		String dst = "hdfs://nameservice1/user/linzhiqiang/out" ;
		String src = "/home/linzhiqiang/parquet-0.0.1-SNAPSHOT.jar" ;
		boolean status = false ;
		
		
		Configuration conf = new Configuration() ;
		status = put2HSFS( src ,  dst ,  conf) ;
		System.out.println("status="+status) ;
		
//		src = "hdfs://xcloud:9000/user/xcloud/out/loadtable.rb" ;
//		dst = "/tmp/output" ;
//		status = getFromHDFS( src ,  dst ,  conf) ;
//		System.out.println("status="+status) ;
//		
//		src = "hdfs://xcloud:9000/user/xcloud/out/loadtable.rb" ;
//		dst = "/tmp/output/loadtable.rb" ;
//		status = checkAndDel( dst ,  conf) ;
//		System.out.println("status="+status) ;
	}


}