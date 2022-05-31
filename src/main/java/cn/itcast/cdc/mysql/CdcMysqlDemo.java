package cn.itcast.cdc.mysql;

import com.ververica.cdc.connectors.base.source.assigner.splitter.JdbcSourceChunkSplitter;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;

/**
 * 读取 mysql 的binlog 日记
 * 1、配置my.cnf 文件，启动日记可监听
 *      log_bin = mysql-bin
 *      binlog_format = row
 *      gtid_mode = on
 *      enforce_gtid_consistency = on
 */
public class CdcMysqlDemo {

  public static void main(String[] args) throws Exception {

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      // enable checkpoint
      env.enableCheckpointing(3000);
      // mysqlSource
      MySqlSource<String> mySqlSource =
              MySqlSource.<String>builder()
                      /** 默认数据库连接池大小为 20  {@link MySqlSourceOptions.CONNECTION_POOL_SIZE} */
                      .connectionPoolSize(20)
                      /** 默认数据库连接失败重试次数为 3 {@link MySqlSourceOptions.CONNECT_MAX_RETRIES} */
                      .connectMaxRetries(3)
                      /** 默认数据库连接超时时间 30L {@link MySqlSourceOptions.CONNECT_TIMEOUT} */
                      .connectTimeout(Duration.ofSeconds(30L))
                      /** 数据库列表 */
                      .databaseList("flink_cdc_db")
                      /** 数据库属性，例如 snapshot.mode */
                      .debeziumProperties(initProperties())
                      /** 指定序列化 */
                      .deserializer(new JsonDebeziumDeserializationSchema())
                      /**
                       * 分布因子上限 > (MAX(id) - MIN(id) + 1) / rowCount > 分布因子下限
                       * 判断 快照分布式切片读 是否均匀
                       * 默认 {@link MySqlSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND}
                       * {@link com.ververica.cdc.connectors.base.experimental.MySqlChunkSplitter.splitTableIntoChunks}
                       * {@link JdbcSourceChunkSplitter}
                       */
                      .distributionFactorLower(0.05D)
                      /**
                       * 分布因子上限 > (MAX(id) - MIN(id) + 1) / rowCount > 分布因子下限
                       * 判断 快照分布式切片读 是否均匀
                       * 默认 {@link MySqlSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND}
                       */
                      .distributionFactorUpper(1000.0D)
                      /**
                       * The maximum fetch size for per poll when read table snapshot.
                       * 读快照时，最大从缓存队列中拉取的条数
                       *  默认 {@link MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE}
                       */
                      .fetchSize(1024)
                      /**
                       * 复制心跳的间隔时间：
                       * Master 在没有数据的时候，每间隔发送一个心跳包。这样 Slave 就能知道 Master 是不是还正常
                       * 默认 {@link MySqlSourceOptions.HEARTBEAT_INTERVAL}
                       * {@link com.github.shyiko.mysql.binlog.BinaryLogClient.enableHeartbeat}
                       * "set @master_heartbeat_period=" + heartbeatInterval * 1000000
                       */
                      .heartbeatInterval(Duration.ofSeconds(30))
                      .hostname("192.168.88.166")
                      /**
                       * 是否输出 schema 的变化
                       */
                      .includeSchemaChanges(true)
//                      .jdbcProperties()
                      .password("123456")
                      .port(3306)
                      /**
                       * 默认 {@link MySqlSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED}
                       */
                      .scanNewlyAddedTableEnabled(false)
                      .serverId("223344")
                      /**
                       * 默认 {@link MySqlSourceOptions.SERVER_TIME_ZONE}
                       */
                      .serverTimeZone("UTC")
                      /**
                       * The group size of chunk meta, if the meta size exceeds the group size, the meta will be will be divided into multiple groups
                       * 默认 {@link MySqlSourceOptions.CHUNK_META_GROUP_SIZE}
                       */
                      .splitMetaGroupSize(1000)
                      /**
                       * 读表 snapshot 时，切成多片，每个切片的大小
                       * 例如表主键id 为 [1,2,3,4,5,6,7,8,9,10], splitSize=3，各切片区间如下：
                       * (-00,4),[4,7),,[7,10),[10,+00)
                       * 默认 3种切片方式
                       *  MySqlBinlogSplitAssigner  binlog切片，以日记文件位移、gtid 为切分维度
                       *  MySqlSnapshotSplitAssigner  快照切片，以切片键为维度单位
                       *  MySqlHybridSplitAssigner 混合切片（binlog+快照），
                       */
                      .splitSize(8096)
                      /**
                       * 多个个表结构必须一致
                       */
                      .tableList("flink_cdc_db.customers_1", "flink_cdc_db.customers_2")
                      .username("root")
                      .build();

      // set the source parallelism to 4
      env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlParallelSource")
              .setParallelism(4)
              .print();

      env.execute("Print MySQL Snapshot + Binlog");
  }

  public static Properties initProperties(){
      Properties properties = new Properties();

      /** {@link com.ververica.cdc.connectors.mysql.table.StartupMode} */

      /**
       * 一旦重新连接，就读快照 和 binlog
       * {@link com.ververica.cdc.connectors.mysql.table.StartupMode.INITIAL}
       */
      properties.put("snapshot.mode", "initial");

      /**
       * 一旦重新连接，不会读快照，只读binlog
       * {@link com.ververica.cdc.connectors.mysql.table.StartupMode.EARLIEST_OFFSET}
       * 一旦重新连接，不会读快照，只读指定时间timestamp的binlog
       * {@link com.ververica.cdc.connectors.mysql.table.StartupMode.TIMESTAMP}
       */
      properties.put("snapshot.mode", "never");

      /**
       * 一旦重新连接，不会读快照，只读 连接后的 binlog
       * {@link com.ververica.cdc.connectors.mysql.table.StartupMode.LATEST_OFFSET}
       */
      properties.put("snapshot.mode", "schema_only");

      /**
       *  一旦重新连接，不会读快照，只读指定位移的binlog
       * {@link com.ververica.cdc.connectors.mysql.table.StartupMode.SPECIFIC_OFFSETS}
       */
      properties.put("snapshot.mode", "schema_only_recovery");
      return properties;
  }
}
