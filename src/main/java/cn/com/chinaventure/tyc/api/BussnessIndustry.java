package cn.com.chinaventure.tyc.api;

import com.jfinal.plugin.activerecord.ActiveRecordPlugin;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Record;
import com.jfinal.plugin.druid.DruidPlugin;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BussnessIndustry{
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
                    String one_level = r3.get("one_level");
                    String two_level = r3.get("two_level");
                    String  three_level= r3.get("three_level");
                    String one_level_name = r3.get("one_level_name");
                    String two_level_name = r3.get("two_level_name");
                    String  three_level_name= r3.get("three_level_name");
                    Integer  industry_type= r3.get("industry_type");
                    //先删掉自动添加的数据
                    Record record=new Record();
                    record.set("one_level",one_level);
                    record.set("two_level",two_level);
                    record.set("three_level",three_level);
                    record.set("one_level_name",one_level_name);
                    record.set("two_level_name",two_level_name);
                    record.set("three_level_name",three_level_name);
                    record.set("business_id",business_id);
                    record.set("company_id",company_id);
                    record.set("business_type",business_type);
                    record.set("industry_type",industry_type);
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
        List<Record> recordList = Db.use(DB_CMP_NAME).find("select count(1) datacount,one_level,one_level_name,two_level,two_level_name,three_level,three_level_name,industry_type  from  "+DB_CMP_NAME+".tbl_com_industry where business_id='"+business_id+"' and company_id='"+company_id+"' and business_type='1' and status='1' and industry_type in (1,2) group by one_level,two_level,three_level");
        return recordList;
    }


    /**
     * 根据公司ID和投资方ID 删除已有自动统计数据
     * @return
     */
    public static void delByCompIdAndBussId(Record record)

    {
        String one_level = record.get("one_level");
        String two_level = record.get("two_level");
        String three_level = record.get("three_level");
        Long business_id = record.get("business_id");
        Long company_id = record.get("company_id");
        Integer business_type = record.get("business_type");
        Integer industry_type = record.get("industry_type");
        //1标识自动添加
        Integer operate_type = record.get("operate_type");
        Db.use(DB_CMP_NAME).update("update tbl_com_industry set status='2' where one_level=? and two_level=? and three_level=? and business_id=? and company_id=? and business_type=? and operate_type=? and industry_type=?",one_level,two_level,three_level,business_id,company_id,business_type,operate_type,industry_type);
    }
    /**
     * 根据 条件查询
     * @return
     */
    public static List<Record>  findByCompIdAndBussId(Record record)

    {
        String one_level = record.get("one_level");
        String two_level = record.get("two_level");
        String three_level = record.get("three_level");
        Long business_id = record.get("business_id");
        Long company_id = record.get("company_id");
        Integer business_type = record.get("business_type");
        Integer industry_type = record.get("industry_type");
        //1标识自动添加
        Integer operate_type = record.get("operate_type");
        List<Record> recordList = Db.use(DB_CMP_NAME).find("select * from tbl_com_industry where one_level=? and two_level=? and three_level=? and business_id=? and company_id=? and business_type=? and operate_type=? and industry_type=?",one_level,two_level,three_level,business_id,company_id,business_type,operate_type,industry_type);
    return recordList;
    }
    /**
     * 更新记录
     * @return
     */
    public static void upRecordByID(Record record){
        Db.use(DB_CMP_NAME).update("tbl_com_industry",record);
    }

    /**
     * 保存记录
     * @return
     */
    public static void insertRecord(Record record){
        Db.use(DB_CMP_NAME).save("tbl_com_industry",record);
    }



}

