package org.hadoopClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;


public class HdfsClient {

    static Logger LOGGER = LogManager.getLogger(HdfsClient.class);

    /**
     * 參數優先級：
     * 1 代碼裏面的配置 (最高)
     * 2 在項目資源目錄下的配置文件
     * 3 hdfs-site.xml (java專案裡的resources)
     * 4 hdfs-default.xml* (hadoop裡預設的)
     */

    private FileSystem fileSystem;

    @Before
    public void initFileSystem() throws URISyntaxException, IOException, InterruptedException {

        // 連接集群的nn地址
        URI uri = new URI("hdfs://localhost:8020");


        // 創建一個配置文件 *這裡的配置最高級 會覆蓋 Hadoop預設的
        Configuration config = new Configuration();

        // 修改副本數
        config.set("dfs.replication", "2");
        config.set("dfs.client.socket-timeout", "300000");

        //---------------------- 讓data node 對外訪問 ------------------------------------
        //客戶端用 hostname 去連 DataNode。
        config.set("dfs.client.use.datanode.hostname", "true");
        //DataNode用 hostname 向外宣告。
        config.set("dfs.datanode.use.datanode.hostname", "true");
        //----------------------------------------------------------




        //所屬用戶
        String user = "root";

        //獲取到了客戶端對象
        this.fileSystem = FileSystem.get(uri,config,user);

    }


    @After
    public void close() throws IOException {
        //關閉資源
        this.fileSystem.close();
    }



    //創建路徑
    @Test
    public void testMkdirs() throws IOException {

        Path path = new Path("/xiyou/huaguoshang");

        //創建一個文件夾
        this.fileSystem.mkdirs(path);
    }



    //上傳
    @Test
    public void testUpload() throws IOException {
        //delSrc 上傳後是否刪除原始資源(本地)
        //overwrite 是否覆蓋(Hadoop上)
        //src 上傳資源地址
        //dst 上傳目標地址
        Path path = new Path("/Users/sai/IdeaProjects/Spark/src/main/java/org/hadoopClient/te22.txt");
        Path dst = new Path("/xiyou/huaguoshang");
        this.fileSystem.copyFromLocalFile(false,true,path,dst);
    }

    //下載
    @Test
    public void testDownload() throws IOException {


        //delSrc 下載後是否刪除資源(Hadoop上)
        //src Hadoop 資源路徑
        //dst 下載到本地路徑
        //useRawLocalFileSystem 下載後是否驗證數據完整

        Path src = new Path("/xiyou/huaguoshang");
        Path dst = new Path("/Users/sai/IdeaProjects/Spark/src/main/java/org/hadoopClient/test99/");
        this.fileSystem.copyToLocalFile(false,src,dst,true);

    }

    //刪除 空目錄 文件 非空目錄
    @Test
    public void testDelete() throws IOException {

        //Path 刪除的目錄 或文件
        //recursive 遞歸刪除 非空目錄 true 會把李變的東西寫清空
        Path path = new Path("/xiyou/huaguoshang");
        this.fileSystem.delete(path,false);
    }


    //移動 改名
    @Test
    public void testrename() throws IOException {

        //src 移動.改名前的目標
        //dst 想要移動.改名的結果
        Path src = new Path("/xiyou/huaguoshang");
        Path dst = new Path("/heew");
        this.fileSystem.rename(src,dst);
    }

    //詳細訊息
    @Test
    public void testDetail() throws IOException {

        Path path = new Path("/");

        //path 想查詢的起點路徑
        //recursive 是否遞歸

        RemoteIterator<LocatedFileStatus> files = this.fileSystem.listFiles(path,true);

        while (files.hasNext()) {

            LocatedFileStatus locatedFileStatus = files.next();

            System.out.println("======"+locatedFileStatus.getPath()+"====");
            System.out.println(locatedFileStatus.getPermission());
            System.out.println(locatedFileStatus.getOwner());
            System.out.println(locatedFileStatus.getGroup());
            System.out.println(locatedFileStatus.getLen());
            System.out.println(locatedFileStatus.getModificationTime());
            System.out.println(locatedFileStatus.getReplication());
            System.out.println(locatedFileStatus.getBlockSize());
            System.out.println(locatedFileStatus.getPath().getName());

            System.out.println(Arrays.toString(locatedFileStatus.getBlockLocations()));


        }

    }



//  判斷是否是文件或路徑
    @Test
    public void testListStatus() throws IOException {
//        listStatusRecursion(new Path("/"));
        listStatusIterative(new Path("/"));
    }


    //遞歸
    public void listStatusRecursion(Path path) throws IOException {
        FileStatus[] fileStatuses = this.fileSystem.listStatus(path);

        for (FileStatus fileStatus : fileStatuses) {

            System.out.println("========"+fileStatus.getPath().getName()+"========");

            if (fileStatus.isFile()){
                System.out.println("是文件");
            }else {
                System.out.println("是路徑");
                listStatusRecursion(fileStatus.getPath());
            }
        }

    }

    //迭代
    public void listStatusIterative(Path path) throws IOException {
        Queue<Path> queue = new LinkedList<>();
        queue.add(path);

        while (!queue.isEmpty()) {
            Path currentPath = queue.poll(); // 取出當前目錄
            try {
                FileStatus[] fileStatuses = fileSystem.listStatus(currentPath); // 可能拋異常
                for (FileStatus fileStatus : fileStatuses) {
                    System.out.println("======== " + fileStatus.getPath().getName() + " ========");

                    if (fileStatus.isFile()) {
                        System.out.println("是文件");
                    } else {
                        System.out.println("是目錄");
                        queue.add(fileStatus.getPath());  // 迭代方式處理子目錄，避免遞歸
                    }
                }
            } catch (IOException e) {
                System.err.println("無法訪問：" + currentPath + "，錯誤：" + e.getMessage());
                // 這裡記錄錯誤，繼續處理下一個路徑
            }
        }
    }



}
