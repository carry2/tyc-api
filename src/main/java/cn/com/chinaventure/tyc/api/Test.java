package cn.com.chinaventure.tyc.api;

import cn.com.chinaventure.tyc.api.common.bean.PostResult;
import com.jfinal.json.FastJson;
import com.jfinal.kit.HttpKit;
import com.jfinal.kit.StrKit;
import com.jfinal.plugin.activerecord.ActiveRecordPlugin;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.DbPro;
import com.jfinal.plugin.activerecord.Page;
import com.jfinal.plugin.activerecord.Record;
import com.jfinal.plugin.druid.DruidPlugin;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

  public class Test{
	private static Logger log = LogManager.getLogger(Test.class);
	private static String DB_QMP_NAME = "db4_prism_qimingpian";
	private static String DB_PRI_NAME = "db1_prism1";
	private static String DB_ETL_NAME = "tianyancha_etl";
	private static String DB_ONLINE_NAME = "datacenter_online";
	private static String DB_CMP_NAME = "datacenter_cmp";
	private static String USER_NAME = "db-cmp";
	private static String USER_PWD = "EOc3syVVqrmAriqs";

	private static String USER_NAME2 = "read-tyc";
	private static String USER_PWD2 = "aaKcKA6iDXyJVeF6";
	private static String TIANYANCHA = "http://39.106.208.169:8080/v1/api/tyc";
	private static String PUBLICWECHAT = "http://open.api.tianyancha.com/services/v4/open/publicWeChat";
	private static String COMRELATIONS = "http://open.api.tianyancha.com/services/v3/open/oneKey/c";
	private static String STOCKSTRUCTURE = "http://open.api.tianyancha.com/services/v4/open/equityRatio";
	private static String NEWS = "http://open.api.tianyancha.com/services/v3/open/news";
	private static String INVESTTREE = "http://open.api.tianyancha.com/services/v3/open/investtree";
	private static String dirup = "up";
	private static String dirdown = "down";
	private static int flag1 = 1;
	private static int flag2 = 2;
	private static int flag3 = 3;
	private static int TotalPage;
	private static int TotalPage2;
	private static int TotalPage3;
	private static int PageSize = 10000;
	private static String CHARSET = "UTF-8";

	static { log.debug("初始化数据库");

		DruidPlugin dr1 = new DruidPlugin("jdbc:mysql://rm-2ze9cx91355z4mk9hpo.mysql.rds.aliyuncs.com:3306/" + DB_CMP_NAME + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false", USER_NAME, USER_PWD, "com.mysql.jdbc.Driver");
		ActiveRecordPlugin arp1 = new ActiveRecordPlugin(DB_CMP_NAME, dr1);
		dr1.start();
		arp1.start();

		DruidPlugin dr3 = new DruidPlugin("jdbc:mysql://223.71.128.110:18506/" + DB_PRI_NAME + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false", USER_NAME2, USER_PWD2, "com.mysql.jdbc.Driver");
		ActiveRecordPlugin arp3 = new ActiveRecordPlugin(DB_PRI_NAME, dr3);
		dr3.start();
		arp3.start();

		DruidPlugin dr4 = new DruidPlugin("jdbc:mysql://rm-2ze9cx91355z4mk9hpo.mysql.rds.aliyuncs.com:3306/" + DB_ETL_NAME + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false", USER_NAME, USER_PWD, "com.mysql.jdbc.Driver");
		ActiveRecordPlugin arp4 = new ActiveRecordPlugin(DB_ETL_NAME, dr4);
		dr4.start();
		arp4.start();
	}










	public static void main(String[] args)
	{
		Page<Record> paginate = Db.use(DB_ETL_NAME).paginate(1, PageSize, "SELECT  name company ", "FROM " + DB_ETL_NAME + ".tmp_order_company_gs_not_cvs_4");
		TotalPage = paginate.getTotalPage();
		System.out.println("开始处理微信公众号公司新闻 总页数：" + TotalPage + " 总条数：" + paginate.getTotalRow());
		for (int p = 288; p <= TotalPage; p++)
		{
			saveCompanysWechatsInfo(p);

			saveCompanysNews(p);
		}
		System.out.println("成功处理微信公众号公司新闻");

		Page<Record> paginate2 = Db.use(DB_ETL_NAME).paginate(1, PageSize, "SELECT   name company ", "FROM " + DB_ETL_NAME + ".tmp_order_company_gs_not_cvs_4");
		TotalPage2 = paginate2.getTotalPage();
		System.out.println("开始处理数据关系图谱股权结构图 总页数：" + TotalPage2 + " 总条数：" + paginate2.getTotalRow());
		for (int k = 1; k <= TotalPage2; k++)
		{
			saveCompanysRelationsInfo(k);

			saveCompanysStockStructure(k);
		}
		System.out.println("成功处理数据关系图谱股权结构图");



		Page<Record> paginate3 = Db.use(DB_ETL_NAME).paginate(1, PageSize, "SELECT  name company", "FROM " + DB_ETL_NAME + ".tmp_order_company_gs_not_cvs_4 ");
		TotalPage3 = paginate3.getTotalPage();
		System.out.println("开始处理所有公司投资族谱 总页数：" + TotalPage3 + " 总条数：" + paginate3.getTotalRow());
		for (int j = 1; j <= TotalPage3; j++)
		{
			saveCompanysAllInvestTree(j);
		}
		System.out.println("成功处理所有公司投资族谱");
		System.out.println("成功处理所有信息 表名tmp_order_company_gs_not_cvs_4");
	}





	public static char getRandomChar()
	{
		return (char)(19968 + (int)(Math.random() * 20902.0D));
	}



	private static int cvs()
	{
		String sql = "SELECT b.`name`, 'cvs' source, a.tianyancha_id as graph_id, NOW() as create_time FROM " + DB_ONLINE_NAME + ".cmp_enterprise_tianyancha a JOIN " + DB_ONLINE_NAME + ".dc_enterprise b ON a.enterprise_id = b.id";
		return Db.use(DB_CMP_NAME).update("REPLACE INTO " + DB_CMP_NAME + ".tyc_api_company " + sql);
	}


	private static int qimingpian()
	{
		String sql = "SELECT DISTINCT company_name company , 'qmp' as source, null as graph_id, NOW() as create_time FROM " + DB_QMP_NAME + ".qimingpian_history_rongzi UNION  SELECT DISTINCT company, 'qmp' as source, null as graph_id, NOW() as create_time  FROM " + DB_QMP_NAME + ".qimingpian_jigou_tzanli  UNION   SELECT DISTINCT company_name  company, 'qmp' as source, null as graph_id, NOW() as create_time  FROM " + DB_QMP_NAME + ".qimingpian_jingpin_map  UNION   SELECT DISTINCT company_name company, 'qmp' as source, null as graph_id, NOW() as create_time  FROM " + DB_QMP_NAME + ".qimingpian_product  UNION   SELECT DISTINCT company_name company, 'qmp' as source, null as graph_id, NOW() as create_time  FROM " + DB_QMP_NAME + ".qimingpian_team_member  ";

		log.debug(sql);
		List<Record> recordList = Db.use(DB_QMP_NAME).find(sql);
		try
		{
			Db.use(DB_CMP_NAME).batch("REPLACE INTO " + DB_CMP_NAME + ".tyc_api_company values(?, ?, ?, ? )", "company, source, graph_id, create_time", recordList, 5000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return recordList.size();
	}





	public static List<Record> findAllCompanys(int p)
	{
		Page<Record> paginate = Db.use(DB_ETL_NAME).paginate(p, PageSize, "SELECT  name company ", "FROM " + DB_ETL_NAME + ".tmp_order_company_gs_not_cvs_4");
		List<Record> recordList = paginate.getList();


		for (Record r : recordList) {
			String name = r.get("company").toString();
			String sql = "SELECT graph_id FROM " + DB_PRI_NAME + ".company_graph g  JOIN " + DB_PRI_NAME + ".company c on g.company_id=c.id and c.name='" + name + "'";
			List<Record> recordList3 = Db.use(DB_PRI_NAME).find(sql);
			if (recordList3.size() > 0) {
				r.set("graph_id", ((Record)recordList3.get(0)).get("graph_id") == null ? "" : ((Record)recordList3.get(0)).get("graph_id").toString());
			}
		}
		return recordList;
	}







	public static List<Record> findAllCompanys2(int p)
	{
		Page<Record> paginate = Db.use(DB_ETL_NAME).paginate(p, PageSize, "SELECT  name company ", "FROM " + DB_ETL_NAME + ".tmp_order_company_gs_not_cvs_4");
		List<Record> recordList = paginate.getList();

		List<Record> recordList2 = new ArrayList();
		for (Record r : recordList) {
			String name = r.get("company").toString();
			String sql = "SELECT graph_id FROM " + DB_PRI_NAME + ".company_graph g  JOIN " + DB_PRI_NAME + ".company c on g.company_id=c.id and c.name='" + name + "'";
			List<Record> recordList3 = Db.use(DB_PRI_NAME).find(sql);
			if ((recordList3.size() > 0) &&
					(((Record)recordList3.get(0)).get("graph_id") != null)) {
				r.set("graph_id", ((Record)recordList3.get(0)).get("graph_id").toString());
				recordList2.add(r);
			}
		}

		return recordList2;
	}




	public static int saveCompanysWechatsInfo(int p)
	{
		List<Record> allCompany = findAllCompanys(p);


		for (Record record : allCompany) {
			String name = (String)record.get("company");
			String id = record.get("graph_id") == null ? "" : record.get("graph_id").toString();


			String url = "{\n  \"url\": \"" + PUBLICWECHAT + "?id=" + id + "&name=" + reCompanyName(name) + "&pageSize=1000000000\"\n}";

			String postResult = getPostResult(TIANYANCHA, url);
			PostResult parse = (PostResult)FastJson.getJson().parse(postResult, PostResult.class);
			record.set("reason", parse == null ? "请求失败" : parse.getReason());
			record.set("error_code", parse == null ? null : parse.getError_code());
			record.set("result", parse == null ? null : parse.getResult());
		}
		try {
			Db.use(DB_CMP_NAME).batch("REPLACE INTO " + DB_CMP_NAME + ".tyc_api_company_wechat values(?, ?, ?, ?, ? )", "company, graph_id, reason, error_code,result", allCompany, 5000);
			System.out.println("微信公众号信息：第" + p + "页处理完毕");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}





	public static int saveCompanysRelationsInfo(int p)
	{
		List<Record> allCompany = findAllCompanys2(p);


		for (Record record : allCompany)
		{

			String url = "{\n  \"url\": \"" + COMRELATIONS + "?id=" + record.get("graph_id").toString() + "\"\n}";

			String postResult = getPostResult(TIANYANCHA, url);
			PostResult parse = (PostResult)FastJson.getJson().parse(postResult, PostResult.class);
			record.set("reason", parse == null ? "请求失败" : parse.getReason());
			record.set("error_code", parse == null ? null : parse.getError_code());
			record.set("result", parse == null ? null : parse.getResult());
		}
		try
		{
			Db.use(DB_CMP_NAME).batch("REPLACE INTO " + DB_CMP_NAME + ".tyc_api_company_relations values(?, ?, ?, ?, ? )", "company, graph_id, reason, error_code,result", allCompany, 5000);
			System.out.println("关系图谱信息：第" + p + "页处理完毕");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}





	public static int saveCompanysStockStructure(int p)
	{
		List<Record> allCompany = findAllCompanys2(p);


		for (Record record : allCompany)
		{


			String url = "{\n  \"url\": \"" + STOCKSTRUCTURE + "?id=" + record.get("graph_id").toString() + "\"\n}";

			String postResult = getPostResult(TIANYANCHA, url);
			PostResult parse = (PostResult)FastJson.getJson().parse(postResult, PostResult.class);
			record.set("reason", parse == null ? "请求失败" : parse.getReason());
			record.set("error_code", parse == null ? null : parse.getError_code());
			record.set("result", parse == null ? null : parse.getResult());
		}


		try
		{
			Db.use(DB_CMP_NAME).batch("REPLACE INTO " + DB_CMP_NAME + ".tyc_api_company_stockstructure values(?, ?, ?, ?, ? )", "company, graph_id, reason, error_code,result", allCompany, 5000);
			System.out.println("股权结构图信息：第" + p + "页处理完毕");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}




	public static int saveCompanysNews(int p)
	{
		List<Record> allCompany = findAllCompanys(p);

		for (Record record : allCompany) {
			String name = (String)record.get("company");


			String url = "{\n  \"url\": \"" + NEWS + "?&name=" + reCompanyName(name) + "\"\n}";

			String postResult = getPostResult(TIANYANCHA, url);
			PostResult parse = (PostResult)FastJson.getJson().parse(postResult, PostResult.class);
			record.set("reason", parse == null ? "请求失败" : parse.getReason());
			record.set("error_code", parse == null ? null : parse.getError_code());
			record.set("result", parse == null ? null : parse.getResult());
		}

		try
		{
			Db.use(DB_CMP_NAME).batch("REPLACE INTO " + DB_CMP_NAME + ".tyc_api_company_news values(?, ?, ?, ?, ? )", "company, graph_id, reason, error_code,result", allCompany, 5000);
			System.out.println("公司新闻：第" + p + "页处理完毕");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}




	public static void saveCompanysAllInvestTree(int p)
	{
		saveCompanysInvestTree(flag1, dirup, p);
		saveCompanysInvestTree(flag1, dirdown, p);
		saveCompanysInvestTree(flag2, dirup, p);
		saveCompanysInvestTree(flag2, dirdown, p);
		saveCompanysInvestTree(flag3, dirup, p);
		saveCompanysInvestTree(flag3, dirdown, p);
	}




	public static int saveCompanysInvestTree(int flag, String dir, int p)
	{
		List<Record> allCompany = findAllCompanys2(p);


		for (Record record : allCompany)
		{


			String url = "{\n  \"url\": \"" + INVESTTREE + "?id=" + record.get("graph_id").toString() + "&flag=" + flag + "&dir=" + dir + "\"\n}";

			String postResult = getPostResult(TIANYANCHA, url);
			PostResult parse = (PostResult)FastJson.getJson().parse(postResult, PostResult.class);
			record.set("reason", parse == null ? "请求失败" : parse.getReason());
			record.set("error_code", parse == null ? null : parse.getError_code());
			record.set("result", parse == null ? null : parse.getResult());
			record.set("flag", Integer.valueOf(flag));
			record.set("dir", dir);
		}

		try
		{
			Db.use(DB_CMP_NAME).batch("INSERT INTO " + DB_CMP_NAME + ".tyc_api_company_investtree values(?, ?, ?, ?, ? ,?,?)", "company, graph_id, reason, error_code,result,flag,dir", allCompany, 5000);
			System.out.println("投资族谱：flag：" + flag + "dir:" + dir + "第" + p + "页");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}






	public static String getPostResult(String url, String data)
	{
		Map<String, String> headers = new HashMap();
		headers.put("accept", "application/json");
		headers.put("Content-Type", "application/json");
		String result = null;
		try {
			result = HttpKit.post(url, data, headers);
		} catch (Exception e) {
			log.error("发送post请求失败：链接" + url + "参数：" + data);

			return result;
		}
		return result;
	}





	public static String reCompanyName(String value)
	{
		if (StrKit.notBlank(value)) {
			try {
				value.trim();
				value = URLEncoder.encode(value, CHARSET);
			} catch (UnsupportedEncodingException var9) {
				throw new RuntimeException(var9);
			}
		}
		return value;
	}
}

