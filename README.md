# Spark 

HDFS適合場景:
-  一次寫入 多次讀出
<br />

HDFS不適合場景:
-  低延遲數據訪問 匹如毫秒級數據存儲
-  大量小文件
-  不支持併發寫入
-  僅支持數據追加 不支持隨機修改
<br />

## 啟動Docker Hadoop
#### 宿主機 hosts 添加 datanode
sudo vi /etc/hosts
<br />
127.0.0.1 datanode01
<br />
127.0.0.1 datanode02
<br />
sudo dscacheutil -flushcache
<br />

#### JACA客戶端連接Hadoop Datanode需要配置對外返回(內部原本返回docker 容器ip無法)
客戶端用 hostname 去連 DataNode。
<br />
config.set("dfs.client.use.datanode.hostname", "true");
<br />
DataNode用 hostname 向外宣告。
<br />
config.set("dfs.datanode.use.datanode.hostname", "true");

## 常用端口:

| 端口號   | 對應服務                                    | 功能                         |
| ----- | --------------------------------------- | -------------------------- |
| 9870  | HDFS NameNode Web UI                    | 查看 HDFS 狀態                 |
| 8020  | HDFS RPC                                | 客戶端連線 HDFS 的入口             |
| 10020 | MapReduce JobHistory RPC                | 客戶端查詢歷史任務                  |
| 19888 | MapReduce JobHistory Web UI             | 查看 MapReduce 歷史紀錄          |
| 8088  | YARN ResourceManager Web UI             | 查看 YARN 資源與任務              |
| 8030  | YARN Application Manager                | Application 提交入口           |
| 8031  | YARN ResourceManager <-> AM 通訊          | 作業管理                       |
| 8032  | YARN Scheduler RPC                      | 資源調度                       |
| 8033  | YARN 其他 RPC                             | 保留端口                       |
| 8042  | YARN NodeManager Web UI                 | 查看 NodeManager 狀態          |
| 9864  | HDFS DataNode Web UI / WebHDFS REST API | 查看 DataNode / REST API     |
| 8080  | Spark Master Web UI                     | Spark 集群狀態                 |
| 4040  | Spark Application UI                    | 查看 Spark Job、Stage、Task 詳情 |


error:
Permission denied: user=dr.who, access=WRITE, inode="/":root:supergroup:drwxr-xr-x

直接開放 HDFS 根目錄寫入 (不建議生產用)

如果只是測試，可以放寬 / 目錄的權限：
docker exec -it namenode bash
hdfs dfs -chmod 777 /


進到 DataNode 容器裡檢查 HDFS 狀態：
docker exec -it datanode01 bash
hdfs dfsadmin -report


Client和Hadoop集群的通信步驟如下：
1.Client向NameNode發起文件寫入或讀取的請求
2.NameNode返迴文件在DataNode上的信息
3.根據NameNode提供的信息，向DataNode寫入或讀取數據塊。

### TODO
NameNode節點存放的是文件目錄，也就是文件夾、文件名稱，本地可以通過公網訪問 NameNode，所以可以進行文件夾的創建，當上傳文件需要寫入數據到DataNode時，NameNode 和DataNode 是通過局域網進行通信，NameNode返回地址為 DataNode 的私有 IP，本地無法訪問
