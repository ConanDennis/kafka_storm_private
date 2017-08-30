package byr.common;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2017/8/3.
 */
public class Constants {
	public static final String REDIS_HOST = "slave2";
	public static final String REDIS_PORT = "6379";
	public static final String DEFAULT_REDIS_HOST = "localhost";
	public static final Integer DEFAULT_REDIS_PORT = 6379;
	// 网站每日流量统计redis-map-key
	public static final String EVERYDAY_COUNT_MAP = "everyday_count_map";
	public static final String EVERYDAY_COUNT_KEY = "everyday_count_data";
	// 身份统计redis-map
	public static final String IDENTITY_MAP = "identity_map";
	// 用户访问频率统计redis-zsets(value:用户名)
	public static final String USER_VISIT_ZSETS = "user_visit_zsets";

	public static final String cluster_filter = "D:\\Storm项目相关资料\\log\\temp\\data.log";
    public static final Pattern jc_pattern = Pattern.compile("/jc[^\\s]+");
	public final static List<String> cluster_urls;
	static {
		cluster_urls = new ArrayList<>();
		cluster_urls.add("/index.php");
		cluster_urls.add("/forums.php");
		cluster_urls.add("/torrents.php");
		cluster_urls.add("jc");
		cluster_urls.add("/offers.php");
		cluster_urls.add("/upload.php");
		cluster_urls.add("/viewrequests.php");
		cluster_urls.add("/subtitles.php");
		cluster_urls.add("/usercp.php");
		cluster_urls.add("/topten.php");
		cluster_urls.add("/log.php");
		cluster_urls.add("/rules.php");
		cluster_urls.add("/faq.php");
		cluster_urls.add("/staff.php");
		cluster_urls.add("/badges.php");
		cluster_urls.add("/banner_show.php");
	}

    public static boolean matcher(String req){
        Matcher matcher = jc_pattern.matcher(req);
        if (matcher.find() && StringUtils.isNotBlank(matcher.group(0))){
            System.out.println("===="+matcher.group(0));
            return true;
        }
        return false;
    }

	public static String parseUrl(String req) {
		if (StringUtils.isBlank(req)) {
			return req;
		}
		if (req.contains("?")) {
			return req.substring(0, req.indexOf("?"));
		}
		return req;
	}
	public static void main(String[] args) {
//		String req = "/jc_currentbet_L.php";
//		System.out.println("#############"+parseUrl(req));
//        System.out.println(matcher(parseUrl(req)));
		System.out.println((double) (Math.round(0 /0) / 100.0));
	}
}
