
package cn.com.chinaventure.tyc.api;

import com.jfinal.plugin.activerecord.ActiveRecordPlugin;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Record;
import com.jfinal.plugin.druid.DruidPlugin;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BussnessTag{
    private static Logger log = LogManager.getLogger(BussnessIndustry.class);
    private static String DB_CMP_NAME = "datacenter_cmp";
    private static String USER_NAME = "db-cmp";
    private static String USER_PWD = "EOc3syVVqrmAriqs";
    //投资方对应的为2
    private static Integer business_type = 2;
    //自动计算为1 手动添加为2
    private static Integer operate_type1 = 1;
    private static Integer operate_type2 = 2;
    //显示状态 默认为2
    private static Integer show_status = 2;
    //是否可以更改默认为1
    private static Integer enable_update = 1;
    //删除状态默认为1
    private static Integer status = 1;

    static { log.debug("初始化数据库");

        DruidPlugin dr1 = new DruidPlugin("jdbc:mysql://rm-2ze9cx91355z4mk9hpo.mysql.rds.aliyuncs.com:3306/" + DB_CMP_NAME + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false", USER_NAME, USER_PWD, "com.mysql.jdbc.Driver");
        ActiveRecordPlugin arp1 = new ActiveRecordPlugin(DB_CMP_NAME, dr1);
        dr1.start();
        arp1.start();

    }

    public static void main(String[] args)
    {
        Date d=new Date();
        SimpleDateFormat sp=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //获取所有投资方ID
        List<Record> allBusinessIds = findAllBusinessIds();
        for (Record r:allBusinessIds) {
            Long business_id = r.get("business_id");
            System.out.println(business_id);
            //根据投资方ID获取所有公司ID
            List<Record> allBusinessCompanysId = findAllBusinessCompanysId(business_id);
            for(Record r2:allBusinessCompanysId){
                Long company_id = r2.get("company_id");
                //根据公司ID和投资方ID 进行分类统计
                List<Record> records = fundLevelCount(business_id, company_id);
                for(Record r3:records){
                    Long datacount = r3.get("datacount");
                    Integer tags_type = r3.get("tags_type");
                    Integer tag_id = r3.get("tag_id");
                    String  tag_name= r3.get("tag_name");
                    //先删掉自动添加的数据
                    Record record=new Record();
                    record.set("tags_type",tags_type);
                    record.set("tag_id",tag_id);
                    record.set("tag_name",tag_name);
                    record.set("business_id",business_id);
                    record.set("company_id",company_id);
                    record.set("business_type",business_type);
                    // 1 自动计算标识
                    record.set("operate_type",operate_type1);
                    delByCompIdAndBussId(record);
                    //查询是否有手动添加数据 如果有则更改为当前数据 并且更改为自定添加
                    // 2 手动添加标识
                    record.set("operate_type",operate_type2);
                    List<Record> byCompIdAndBussId = findByCompIdAndBussId(record);
                    //如果有手动添加记录
                    if(byCompIdAndBussId.size()>0){
                        //修改对应记录
                        for (Record r4:byCompIdAndBussId) {
                            //将手动改为自动 将统计个数更新
                            r4.set("data_count",datacount);
                            r4.set("operate_type",operate_type1);
                            String format = sp.format(d);
                            record.set("modify_time",format);
                            upRecordByID(r4);
                        }
                    }else{
                        // m没有则直接插入
                        // 1 自动计算标识
                        record.set("operate_type",operate_type1);
                        record.set("data_count",datacount);
                        //可以更新
                        record.set("show_status",show_status);
                        record.set("enable_update",enable_update);
                        record.set("status",status);
                        String format = sp.format(d);
                        record.set("create_time",format);
                        record.set("modify_time",format);
                        insertRecord(record);
                    }
                }

            }

        }

    }

    /**
     * 获取所有投资方ID
     * @return
     */
    public static List<Record> findAllBusinessIds()
    {
        List<Record> recordList = Db.use(DB_CMP_NAME).find("SELECT  DISTINCT(business_id) business_id  FROM "+DB_CMP_NAME+".tbl_event_invest where business_type='2' and business_id is not null and status='1'");
        return recordList;
    }

    /**
     * 根据投资方ID获取所有公司ID
     * @return
     */
    public static List<Record> findAllBusinessCompanysId(Long business_id)
    {

        List<Record> recordList = Db.use(DB_CMP_NAME).find("select DISTINCT(b.company_id) company_id from  "+DB_CMP_NAME+".tbl_event_invest a LEFT JOIN   "+DB_CMP_NAME+".tbl_event b on a.event_id=b.id where a.status='1' and b.status='1' and a.business_id='"+business_id+"' ");
        return recordList;
    }


    /**
     * 根据公司ID和投资方ID 进行分类统计
     * @return
     */
    public static List<Record> fundLevelCount( Long business_id,Long company_id )
    {
        List<Record> recordList = Db.use(DB_CMP_NAME).find("select count(1) datacount,tags_type,tag_id,tag_name  from  "+DB_CMP_NAME+".tbl_com_tag where business_id='"+business_id+"' and company_id='"+company_id+"' and  status='1'  group by tag_name");
        return recordList;
    }


    /**
     * 根据公司ID和投资方ID 删除已有自动统计数据
     * @return
     */
    public static void delByCompIdAndBussId(Record record)

    {
        Integer tags_type = record.get("tags_type");
        Integer tag_id = record.get("tag_id");
        String tag_name = record.get("tag_name");
        Long business_id = record.get("business_id");
        Long company_id = record.get("company_id");
        Integer business_type = record.get("business_type");
        //1标识自动添加
        Integer operate_type = record.get("operate_type");
        Db.use(DB_CMP_NAME).update("update tbl_com_tag set status='2' where tags_type=? and tag_id=? and tag_name=? and business_id=? and company_id=? and business_type=? and operate_type=? ",tags_type,tag_id,tag_name,business_id,company_id,business_type,operate_type);
    }
    /**
     * 根据 条件查询
     * @return
     */
    public static List<Record>  findByCompIdAndBussId(Record record)

    {
        Integer tags_type = record.get("tags_type");
        Integer tag_id = record.get("tag_id");
        String tag_name = record.get("tag_name");
        Long business_id = record.get("business_id");
        Long company_id = record.get("company_id");
        Integer business_type = record.get("business_type");
        //1标识自动添加
        Integer operate_type = record.get("operate_type");
        List<Record> recordList = Db.use(DB_CMP_NAME).find("select * from tbl_com_tag where tags_type=? and tag_id=? and tag_name=? and business_id=? and company_id=? and business_type=? and operate_type=? ",tags_type,tag_id,tag_name,business_id,company_id,business_type,operate_type);
        return recordList;
    }
    /**
     * 更新记录
     * @return
     */
    public static void upRecordByID(Record record){
        Db.use(DB_CMP_NAME).update("tbl_com_tag",record);
    }

    /**
     * 保存记录
     * @return
     */
    public static void insertRecord(Record record){
        Db.use(DB_CMP_NAME).save("tbl_com_tag",record);
    }



}

