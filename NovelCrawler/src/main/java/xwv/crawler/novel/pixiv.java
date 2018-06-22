package xwv.crawler.novel;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import okhttp3.Dns;
import okhttp3.Headers;
import org.apache.commons.lang.StringEscapeUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.sqlite.JDBC;
import org.sqlite.SQLiteDataSource;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisDataException;
import xwv.bean.Response;
import xwv.crawler.Crawler;
import xwv.database.AutoCloseDataSource;
import xwv.database.DataSourceCallBack;
import xwv.database.DataSourcePreparedExecutor;
import xwv.jedis.AutoCloseJedisPool;
import xwv.jedis.JedisCaller;
import xwv.proxy.Proxy;
import xwv.proxy.service.ProxyPoolService;
import xwv.util.ExceptionUtil;
import xwv.util.GzipUtil;
import xwv.util.UnicodeUtil;

import javax.net.ssl.HttpsURLConnection;
import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class pixiv extends Crawler<pixiv.PixivNovel> {
    private Lock lock = new ReentrantLock(true);

    private static class Console extends JFrame {
        private static Dimension ScreenSize = Toolkit.getDefaultToolkit()
                .getScreenSize();
        private final JTextArea textPane;


        public Console() {
            int width = 500;
            int height = 300;
            setTitle("Pixiv Novel Crawler");
            setBounds((ScreenSize.width - width) / 2, (ScreenSize.height - height) / 2, width,
                    height);
            setResizable(false);
            setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
            getContentPane().setLayout(new BorderLayout(0, 0));
            JPanel panel = new JPanel();
            getContentPane().add(panel);

            GridBagLayout gbl_panel = new GridBagLayout();
            gbl_panel.columnWidths = new int[]{width};
            gbl_panel.rowHeights = new int[]{height};
            gbl_panel.columnWeights = new double[]{1};
            gbl_panel.rowWeights = new double[]{1};
            panel.setLayout(gbl_panel);

            JScrollPane scrollPane = new JScrollPane();
            GridBagConstraints gbc_scrollPane = new GridBagConstraints();
            gbc_scrollPane.gridwidth = 1;
            gbc_scrollPane.gridheight = 1;
            gbc_scrollPane.fill = GridBagConstraints.BOTH;
            gbc_scrollPane.insets = new Insets(height / 20, height / 20, height / 20, height / 20);
            gbc_scrollPane.gridx = 0;
            gbc_scrollPane.gridy = 0;
            panel.add(scrollPane, gbc_scrollPane);

            textPane = new JTextArea();
            textPane.setFont(new Font("Monospaced", Font.PLAIN, 12));
            textPane.setEditable(false);

            scrollPane.setViewportView(textPane);
            setVisible(true);


        }

        public void ClearAndPrint(Object o) {
            cls();
            print(o);
        }

        public void cls() {
            EventQueue.invokeLater(new Runnable() {
                @Override
                public void run() {
                    textPane.setText("");
                }
            });
        }

        public void println(Object o) {
            print("\n" + o);
        }

        public void print(Object o) {
            EventQueue.invokeLater(new Runnable() {
                @Override
                public void run() {
                    textPane.setText(textPane.getText() + o);
                }
            });
        }

    }

    private Console console;

    private AutoCloseJedisPool jedis;

    public pixiv() {
        jedis = new AutoCloseJedisPool(new JedisPoolConfig() {{
            setMaxIdle(1000);
            setMaxTotal(1000);
        }}, "127.0.0.1");

        try {
            jedis.call(new JedisCaller<String>() {
                @Override
                public String call(Jedis jedis) {
                    return jedis.ping();
                }
            });
        } catch (JedisDataException e) {
            e.printStackTrace(System.out);
            jedis.close();
            jedis.destroy();
            jedis = new AutoCloseJedisPool(new JedisPoolConfig() {{
                setMaxIdle(1000);
                setMaxTotal(1000);
            }}, "67.218.158.124", "OmegaX!!");

        }

        System.out.println("Ping Redis:" + jedis.call(new JedisCaller<String>() {
            @Override
            public String call(Jedis jedis) {
                return jedis.ping();
            }
        }));

//        console = new Console();
    }

    public pixiv(boolean useProxy) {
        this();
        this.useProxy = useProxy;
    }

    public pixiv(boolean useProxy, boolean forward) {
        this();
        this.useProxy = useProxy;
        this.forward = forward;
        this.step = 1;
    }

    private boolean forward = false;


    public static class PixivNovel {
        private int PixivID;
        private String author;
        private int authorID;

        private String title;
        private boolean isR18;
        private String content;
        private long length;

        private long uploadTime;

        private int rating_count;
        private int bookmark_count;
        private int comment_count;

        public void setCounts(int rating_count, int bookmark_count, int comment_count) {
            this.rating_count = rating_count;
            this.bookmark_count = bookmark_count;
            this.comment_count = comment_count;
        }


        public int getRatingCount() {
            return rating_count;
        }

        public void setRatingCount(int rating_count) {
            this.rating_count = rating_count;
        }

        public int getBookmarkCount() {
            return bookmark_count;
        }

        public void setBookmarkCount(int bookmark_count) {
            this.bookmark_count = bookmark_count;
        }

        public int getCommentCount() {
            return comment_count;
        }

        public void setCommentCount(int comment_count) {
            this.comment_count = comment_count;
        }

        public long getLength() {
            return length;
        }

        public void setLength(long length) {
            this.length = length;
        }

        private final Set<String> tags = new HashSet<>();

        public int getAuthorID() {
            return authorID;
        }

        public void setAuthorID(int authorID) {
            this.authorID = authorID;
        }

        public long getUploadTime() {
            return uploadTime;
        }

        public void setUploadTime(long uploadTime) {
            this.uploadTime = uploadTime;
        }

        public Set<String> getTags() {
            return tags;
        }

        public void setTag(String tag) {
            if (tag != null && !tag.trim().isEmpty()) {
                tags.add(tag);
            }
        }

        //        private AutoCloseJedisPool jedis = new AutoCloseJedisPool(new JedisPoolConfig() {{
//            setTestOnBorrow(true);
//        }}, "127.0.0.1");

        public PixivNovel(int pixivID) {
            PixivID = pixivID;
        }

        public int getPixivID() {
            return PixivID;
        }

        public void setPixivID(int pixivID) {
            PixivID = pixivID;
        }

        public String getAuthor() {
            return author != null && !author.trim().isEmpty() ? author : "Unknown Author";
        }

        public void setAuthor(String author) {
            this.author = author;
        }

        public String getTitle() {
            return title != null && !title.trim().isEmpty() ? title : "PixivID_" + PixivID;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

        public boolean isR18() {
            return isR18;
        }

        public void setIsR18(boolean r18) {
            isR18 = r18;

        }
    }

//    private static final String JapaneseCharSet = "" +
//            "あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわゐゑをん" +
//            "アイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワヰヱヲン" +
//            "がぎぐげござじずぜぞだぢづでどばびぶべぼぱぴぷぺぽ" +
//            "ガギグゲゴザジズゼゾダヂゾデドバビブベボパピプペポ";
//
//    private static final String JapaneseRegex = "[" + JapaneseCharSet + "]+";
//    private static final String EnglishRegex = "[A-Za-z0-9]+[\\s]+";

    private static final String newPageRegex = ".*\\[newpage].*[\\n]?";


    private static final ProxyPoolService.TestCase testCase = new ProxyPoolService.TestCase() {
        @Override
        public boolean test(Proxy proxy) {
            String html = proxy.Post("https://www.pixiv.net", getHeaders(), false).toString();
            if (html != null) {
                Document document = Jsoup.parse(html);
                if (document.selectFirst(".site-name").text().equals("pixiv")) {
                    return true;
                }
            }
            return false;
        }
    };


    private static final Dns dns = new Dns() {

        private final Set<String> hostSet = new HashSet<>();

        private final List<InetAddress> hosts = new ArrayList<InetAddress>() {
            {
                File file = new File("hosts");
                System.out.println(file.getAbsolutePath());
                try (FileReader fileReader = new FileReader(new File("hosts")); BufferedReader reader = new BufferedReader(fileReader)) {
                    String row;
                    while ((row = reader.readLine()) != null) {
                        String[] arr;
                        if ((arr = row.trim().split("[ ]+")).length > 1) {
                            String host = arr[1].trim();
                            add(InetAddress.getByAddress(host, ConvertIP2Bytes(arr[0])));
                            hostSet.add(host);
                        }
                    }

                } catch (IOException e) {
                    ExceptionUtil.printSingleStackTrace(e);
                }

            }
        };

        @Override
        public List<InetAddress> lookup(String hostname) throws UnknownHostException {
//            System.out.println(hostname);
            if (hostSet.contains(hostname)) {
                return hosts;
            } else {
                return SYSTEM.lookup(hostname);
            }
        }
    };

    private static final Proxy NullProxy = new Proxy(null, 0, dns) {{
        setRate(1000d);
    }};


    private static class ProxyPool {
        private static final ProxyPoolService instance = ProxyPoolService.initInstance(dns, testCase);
    }

    public ProxyPoolService getProxyPool() {
        return ProxyPool.instance;
    }

    private boolean useProxy = true;

//    private static String cookie = "p_ab_id=9; p_ab_id_2=5; device_token=aec1e6b3537bb870f30edc88374f0864; yuid=IzRBSAI56; login_ever=yes; _ga=GA1.2.963691050.1524827456; is_sensei_service_user=1; __gads=ID=b50be561fff862a9:T=1526469927:S=ALNI_MYUCvHBaZpBT7Y0JwfEz0Zajneczg; privacy_policy_agreement=1; c_type=22; a_type=0; b_type=1; PHPSESSID=4659697_58f491454a1b4159edf110cf574d3320; __utmt=1; __utma=235335808.963691050.1524827456.1526481126.1526483142.4; __utmb=235335808.1.10.1526483142; __utmc=235335808; __utmz=235335808.1524827456.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmv=235335808.|2=login%20ever=yes=1^3=plan=normal=1^5=gender=male=1^6=user_id=4659697=1^9=p_ab_id=9=1^10=p_ab_id_2=5=1^11=lang=zh=1; module_orders_mypage=%5B%7B%22name%22%3A%22sketch_live%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22tag_follow%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22recommended_illusts%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22showcase%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22everyone_new_illusts%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22following_new_illusts%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22mypixiv_new_illusts%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22fanbox%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22featured_tags%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22contests%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22user_events%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22sensei_courses%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22spotlight%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22booth_follow_items%22%2C%22visible%22%3Atrue%7D%5D; limited_ads=%7B%22header%22%3A%22%22%7D";

    private static final String headerText = "connection: \n" +
            "accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\n" +
            "accept-encoding:gzip, deflate, sdch, br\n" +
            "accept-language:zh-CN,zh;q=0.8\n" +
            "cookie:p_ab_id=9; p_ab_id_2=5; yuid=IzRBSAI56; login_ever=yes; __gads=ID=b50be561fff862a9:T=1526469927:S=ALNI_MYUCvHBaZpBT7Y0JwfEz0Zajneczg; first_visit_datetime=2018-05-17+19%3A33%3A06; bookmark_tag_type=count; bookmark_tag_order=desc; tag_view_ranking=0xsDLqCEW6~B8KB6p6d7I~KN7uxuR89w~ZTBAtZUDtQ~UX647z2Emo~WcTW9TCOx9~THI8rtfzKo~2QTW_H5tVX~BSSNuJ9-rg~_3oeEue7S7~Q7IPBBXvYh~u8oWTWllxV; OX_plg=pm; stacc_mode=stream2; webp_available=1; is_sensei_service_user=1; login_bc=1; _ga=GA1.2.963691050.1524827456; _gid=GA1.2.2059003773.1527001413; device_token=aec1e6b3537bb870f30edc88374f0864; privacy_policy_agreement=1; module_orders_mypage=%5B%7B%22name%22%3A%22sketch_live%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22tag_follow%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22recommended_illusts%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22showcase%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22everyone_new_illusts%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22following_new_illusts%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22mypixiv_new_illusts%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22fanbox%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22featured_tags%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22contests%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22user_events%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22sensei_courses%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22spotlight%22%2C%22visible%22%3Atrue%7D%2C%7B%22name%22%3A%22booth_follow_items%22%2C%22visible%22%3Atrue%7D%5D; __utma=235335808.963691050.1524827456.1527001381.1527005524.34; __utmc=235335808; __utmz=235335808.1524827456.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmv=235335808.|2=login%20ever=yes=1^3=plan=normal=1^5=gender=male=1^6=user_id=4659697=1^9=p_ab_id=9=1^10=p_ab_id_2=5=1^11=lang=zh=1^20=webp_available=yes=1; limited_ads=%7B%22header%22%3A%22%22%7D; c_type=22; a_type=0; b_type=1; PHPSESSID=4659697_8158471dcf4ea86a6bc71e8a8358296e\n" +
            "user-agent:mobile" +
            "upgrade-insecure-requests:1\n";

    public static Headers getHeaders() {
        return getHeaders(null);
    }

    public static Headers getHeaders(String referer) {

        Headers.Builder builder = new Headers.Builder();
        for (String s : headerText.split("\\n")) {
            builder.add(s);
        }
        if (referer != null && !referer.trim().isEmpty()) {
            builder.add("referer", referer);
        }

        return builder.build();
    }

    public static final String TextStoragePath = "pixiv/novel";

    private Proxy getProxy() {
        Proxy proxy = null;
        if (useProxy) {
            proxy = getProxyPool().getNext();
        }

        if (proxy != null) {
            proxy.setRate(1000d);
        } else {
            proxy = NullProxy;
        }
        return proxy;
    }

    private static int parsePixivIDFromUrl(String url) throws MalformedURLException {
        String[] query = new URL(url).getQuery().split("&");
        for (String s : query) {
            String[] arr = s.split("=");
            if (arr[0].equals("id")) {
                return Integer.parseInt(arr[1]);
            }
        }
        return 0;
    }

    @Override
    public PixivNovel parseOne(String url) {
        int PixivID = 0;

        try {
            PixivID = parsePixivIDFromUrl(url);
        } catch (MalformedURLException e) {
            e.printStackTrace(System.out);
            return null;
        }

        String key = "pixiv.novel.info:" + PixivID;


        boolean HasInfoCache = jedis.call(j -> {
            Map<String, String> map = j.hgetAll(key);
            return map != null && map.size() > 0;
        });
        int finalPixivID = PixivID;
        JedisCaller<Object> onErrorCaller = new JedisCaller<Object>() {
            @Override
            public Object call(Jedis jedis) {
                jedis.hset(String.valueOf(finalPixivID / 1000), String.valueOf(finalPixivID % 1000), String.valueOf(-1));
                if (HasInfoCache) {
                    jedis.del(key);
                }
                return null;
            }
        };


        PixivNovel n = getNovel(url, PixivID, onErrorCaller);


        if (n == null || n.getContent() == null || n.getContent().trim().isEmpty()) {
            return null;
        }

        n.setLength(n.getContent().replaceAll("\\s", "").codePoints().count());

        long len = n.getContent().codePoints().count();
        if (len < 2000) {
            jedis.call(jedis -> {
                jedis.hset(String.valueOf(n.getPixivID() / 1000), String.valueOf(n.getPixivID() % 1000), String.valueOf(0));

//                        jedis.zadd("pixiv.novel:ch_rate", 0, String.valueOf(n.getPixivID()));
                if (HasInfoCache) {
                    jedis.del(key);
                }
                return null;
            });
            return null;
        }

        double ChineseRate = UnicodeUtil.calculateChineseRate(n.getContent());


//                while (!lock.tryLock()) {
//                    System.out.println(Thread.currentThread().getName() + ":tryLock");
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException ignored) {
//
//                    }
//
//                }
//                try (FileWriter writer = new FileWriter("rate.txt", true)) {
//
//                    writer.write(PixivID + ":" + Math.round(ChineseRate * 10000) / 100d + "%\n");
//
//                } catch (IOException e) {
//                    e.printStackTrace(System.out);
//                } finally {
//                    lock.unlock();
//                }

        jedis.call(jedis -> {
            jedis.hset(String.valueOf(n.getPixivID() / 1000), String.valueOf(n.getPixivID() % 1000), String.valueOf(Math.round(ChineseRate * 10000d)));

//                    jedis.zadd("pixiv.novel:ch_rate", ChineseRate, String.valueOf(n.getPixivID()));
            return null;
        });

        PixivNovel novel = null;


        if (ChineseRate > 0.51d) {


            if (!HasInfoCache) {
                Map<String, Map<String, String>> list = ParseUserNovelList(n.getAuthorID());
                jedis.call(jedis -> {
                    for (String id : list.keySet()) {
                        jedis.hmset("pixiv.novel.info:" + id, list.get(id));
                    }
                    return null;
                });
            }
//
            jedis.call(jedis -> {

                Map<String, String> map = jedis.hgetAll("pixiv.novel.info:" + n.getPixivID());
                if (map != null && map.size() > 0) {
                    jedis.hset(key, "r18", String.valueOf(n.isR18()));
                    jedis.hset(key, "upload_time", String.valueOf(n.getUploadTime()));
                    map.put("r18", String.valueOf(n.isR18() ? 1 : 0));
                    map.put("upload_time", String.valueOf(n.getUploadTime()));


                    String sql_insert = CreateInfoPreparedInsertSql();
//                            String sql_delete = "DELETE FROM pixiv_novel_info WHERE id=" + finalPixivID;
                    int affect_count = 0;

                    while (!dataSource.getLock().writeLock().tryLock()) {
                        StackTraceElement stm;
                        System.err.println("tryLock(" + (stm = Thread.currentThread().getStackTrace()[2]).getFileName() + ":" + stm.getLineNumber() + ")");
                    }
                    try {
                        affect_count = dataSource.execute(sql_insert, new DataSourcePreparedExecutor<Integer>() {
                            @Override
                            public Integer execute(PreparedStatement s) {
                                int result = 0;
                                try {

                                    SetInfoInsertPreparedParams(s, map);
                                    result = s.executeUpdate();

                                } catch (SQLException e) {

                                    result = 0;

                                }
                                return result;
                            }
                        });
                    } finally {
                        dataSource.getLock().writeLock().unlock();
                    }

                    if (affect_count > 0) {
                        jedis.del(key);
                    }
                }

                return null;
            });
//                    String sql_find_info = "SELECT * FROM pixiv_novel_info WHERE id=" + n.getPixivID();
//                    Boolean hasInfo = dataSource.query(new DataSourceCallBack<Boolean>() {
//                        @Override
//                        public Boolean callback(ResultSet rs) {
//                            return null;
//                        }
//                    }, sql_find_info);
//                    if (hasInfo == null || !hasInfo) {
//                        Map<String, Map<String, String>> list = ParseUserNovelList(n.getAuthorID());
//
//
//                        dataSource.execute(new DataSourceExecutor<Object>() {
//                            @Override
//                            public Object execute(Statement s) {
//                                try {
//                                    s.getConnection().setAutoCommit(false);
//
//                                    for (String id : list.keySet()) {
//                                        String sql_insert = createInfoInsertSql(list.get(id));
//                                        if (sql_insert != null) {
//                                            s.executeUpdate(sql_insert);
//                                        }
//                                    }
//                                    s.getConnection().commit();
//                                    s.getConnection().setAutoCommit(true);
//                                } catch (SQLException e) {
//                                    e.printStackTrace(System.out);
//
//                                }
//                                return null;
//                            }
//                        });
//                    }
//
//                    try {
//                        dataSource.update("UPDATE pixiv_novel_info SET r18='" + (n.isR18() ? 1 : 0) + "' , upload_time='" + n.getUploadTime() + "'");
//                    } catch (SQLException e) {
//                        e.printStackTrace(System.out);
//                    }


            SaveNovel(n);
            novel = n;
        } else {
            if (HasInfoCache) {
                jedis.call(new JedisCaller<Object>() {
                    @Override
                    public Object call(Jedis jedis) {
                        jedis.del(key);
                        return null;
                    }
                });
            }
        }


        return novel;
    }

    public static void SaveNovel(PixivNovel n) {
        File path = new File(TextStoragePath);
        boolean path_flag = true;
        if (!path.exists()) {
            path_flag = path.mkdirs();
        }
        if (path_flag) {
            try (FileWriter writer = new FileWriter(TextStoragePath + File.separator + n.getPixivID() + ".txt")) {
                writer.write(n.getContent());
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
        }
    }

    public static PixivNovel CreateNovelFromDocument(int PixivID, Document document) {
        if (document == null) {
            return null;
        }

        PixivNovel n = new PixivNovel(PixivID);

        String init_config_src = document.select("#init-config").attr("content");
        String init_config_input_src = document.select("#init-config-input").val();
        String init_config = StringEscapeUtils.unescapeJavaScript(init_config_src);
        String init_config_input = StringEscapeUtils.unescapeJavaScript(init_config_input_src);

        JSONObject init_config_json = JSONObject.fromObject(init_config_src);
        JSONObject init_config_input_json = JSONObject.fromObject(init_config_input_src);


        if (init_config_json != null && init_config_json.has("pixiv.context.r18") && init_config_json.getInt("pixiv.context.r18") == 1) {
            n.setIsR18(true);
        }

        if (init_config_input_json.has("pixiv.context.userId")) {
            n.setAuthorID(init_config_input_json.getInt("pixiv.context.userId"));
        }

        if (init_config_input_json.has("pixiv.context.updated")) {
            n.setUploadTime(init_config_input_json.getLong("pixiv.context.updated"));
        }


//            Elements novel_display_work = document.select("div[class='novel-display work']");
//            n.setTitle(novel_display_work.select("h1 span[class='title']").text());
//            n.setAuthor(novel_display_work.select("h2 a[class='author']").text());

//            n.setAuthor(document.select(".user-name").text());
//            n.setTitle(document.select("h1[class='title']").text());
//


//            String novel_text = document.select("#novel_text").text();

        String novel_text = init_config_input_json.has("pixiv.novel.text") ? init_config_input_json.getString("pixiv.novel.text") : null;


        if (novel_text != null) {
            String[] content = novel_text.split(newPageRegex);
            StringBuilder total = new StringBuilder();
            for (String s : content) {
                total.append(s);
            }
            n.setContent(total.toString());
        }
        return n;
    }

    private PixivNovel getNovel(String url, int PixivID, JedisCaller<Object> onErrorCaller) {


        String html = null;
        boolean retry = false;

        while (html == null) {

//            Response response = OkHttpPost(url, getHeaders(url));
//            Response response = post(url, getHeaders(url));
//            Response response = NullProxy.OkHttpPost(url, getHeaders(url));

            Proxy proxy = getProxy();

            long timestamp = System.currentTimeMillis();
            Response response = proxy.PostForResponse(url, getHeaders(url));
//            System.out.println("net:" + (System.currentTimeMillis() - timestamp));


            if (response != null) {
                if (retry) {
                    System.out.println("Retry Success:" + url);
                }
                if (response.code() < 500) {
                    if (proxy.toString() != null) {
                        getProxyPool().refresh(proxy.toString());
                    }

                    if (response.code() == 404) {
                        AddCount404();
                    } else {
                        RefreshMaxNo404(PixivID);
                    }

                    if (response.code() == 200) {
                        html = response.body();
                    } else {
//                        if (response.code() == 404) {
//                            jedis.call((j) -> j.hdel(String.valueOf(PixivID / 1000), String.valueOf(PixivID % 1000)));
//                        } else {
//                            jedis.call(onErrorCaller);
//                        }
                        jedis.call(onErrorCaller);
                        return null;
                    }


                } else {
                    if (proxy.toString() != null) {
                        getProxyPool().remove(proxy.toString(), (long) 30);
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {

                    }
                }
            } else {
                retry = true;
                if (proxy.toString() != null) {
                    getProxyPool().remove(proxy.toString(), (long) 30);
                }
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ignored) {
                }
            }


        }

        if (html.isEmpty()) {
            return null;
        }


        Document document = Jsoup.parse(html);
        if (document.select("#error_display").size() > 0) {
            jedis.call(onErrorCaller);
            return null;
        }

        return CreateNovelFromDocument(PixivID, document);
    }


    private static final String ColumnStr = "" +
            "id" + "," +
            "title" + "," +
            "user_id" + "," +
            "cover_type" + "," +
            "cover_id" + "," +
            "serialized_value" + "," +
            "tag" + "," +
            "comment" + "," +
            "scene" + "," +
            "restrict" + "," +
            "count" + "," +
            "score" + "," +
            "view" + "," +
            "novel_cover_preset_id" + "," +
            "novel_cover_img_name" + "," +
            "novel_cover_img_ext" + "," +
            "type" + "," +
            "url" + "," +
            "one_comment_count" + "," +
            "bookmark_count" + "," +
            "user_name" + "," +
            "marker_count" + "," +
            "series_id" + "," +
            "series_title" + "," +
            "text_length" + "," +
            "reupload_date" + "," +
            "r18" + "," +
            "upload_time";

    private static String CreateInfoPreparedInsertSql() {


        String[] keyArray = ColumnStr.split(",");
        StringBuilder builder = new StringBuilder();
        for (String k : keyArray) {
            if (builder.length() > 0) {
                builder.append(",");
            }
            builder.append("?");
        }
        return "REPLACE INTO pixiv_novel_info(" + ColumnStr + ") VALUES(" + builder.toString() + ")";


    }

    public static String CreateInfoPreparedUpdateSql() {


        String[] keyArray = ColumnStr.split(",");
        StringBuilder builder = new StringBuilder();
        for (String k : keyArray) {
            if (k.equals("id") || k.equals("r18") || k.equals("upload_time")) {
                continue;
            }
            if (builder.length() > 0) {
                builder.append(",");
            }
            builder.append(k).append("=").append("?");
        }
        return "UPDATE pixiv_novel_info SET " + builder.toString() + " WHERE id=?";


    }

    private static void SetInfoInsertPreparedParams(PreparedStatement s, Map<String, String> map) throws SQLException {
        String[] keyArray = ColumnStr.split(",");
        for (int i = 0; i < keyArray.length; i++) {
            String value = map.get(keyArray[i]);
            if (value != null && !value.equals("null")) {
                s.setString(i + 1, value);
            }
        }
    }


    public static void SetInfoUpdatePreparedParams(PreparedStatement s, Map<String, String> map) throws SQLException {
        String id = map.get("id");
        String[] keyArray = ColumnStr.split(",");
        int i = 0;
        int skip = 0;
        while (i < keyArray.length) {
            String k = keyArray[i];
            if (k.equals("id") || k.equals("r18") || k.equals("upload_time")) {
                skip++;
            } else {
                String value = map.get(k);
                if (value != null && !value.equals("null")) {
                    s.setString(i - skip + 1, value);
                }
            }
            i++;
        }
        s.setString(i - skip + 1, id);
        ParameterMetaData parameterMetaData = s.getParameterMetaData();
    }

    private Map<String, Map<String, String>> ParseUserNovelList(int authorID) {
        Proxy proxy = getProxy();
        return ParseUserNovelList(authorID, proxy);
    }

    public static Map<String, Map<String, String>> ParseUserNovelList(int authorID, Proxy proxy) {
        int page = 1;
        Map<String, Map<String, String>> result = new HashMap<>();

        while (true) {

            Response response = proxy.PostForResponse("https://www.pixiv.net/touch/ajax_api/novel_api.php?mode=user_novel&id=" + authorID + "&p=" + page, getHeaders());
            if (response != null && response.code() == 200) {
                JSONArray array = JSONArray.fromObject(response.body());
                if (array.size() < 1) {
                    break;
                }
                for (Object o : array) {
                    JSONObject json = JSONObject.fromObject(o);
                    Map<String, String> map = new HashMap<>();
                    for (String k : (Set<String>) json.keySet()) {
                        map.put(k, json.getString(k));
                    }
                    result.put(json.getString("id"), map);
                }
            } else {
                break;
            }
            page++;
        }

        return result;
    }

    @Override
    public ConcurrentLinkedQueue<String> nextUrlQueue() {
        return null;
    }

    @Override
    public boolean hasNextUrlQueue() {
        return false;
    }

    @Override
    public void reset() {

    }


    public static byte[] ConvertIP2Bytes(String ip) {
        String[] arr;
        if (ip != null && (arr = ip.split("\\.")
        ).length > 3) {
            return new byte[]{
                    (byte) Integer.parseInt(arr[0]),
                    (byte) Integer.parseInt(arr[1]),
                    (byte) Integer.parseInt(arr[2]),
                    (byte) Integer.parseInt(arr[3])
            };
        } else {
            return null;
        }

    }

    private int count = 0;
    private long time;
    private int time_count = 0;
    private int time_skip = 0;


    private ReadWriteLock RWLock = new ReentrantReadWriteLock(true);


    private double Speed;

    private double Speed() {
        RWLock.readLock().lock();
        try {
            return Speed;
        } finally {
            RWLock.readLock().unlock();
        }
    }

    private double Speed(double value) {
        RWLock.writeLock().lock();
        try {
            Speed = value;
            return Speed;
        } finally {
            RWLock.writeLock().unlock();
        }
    }

    private int Mode() {
        RWLock.readLock().lock();
        try {
            return step > 0 ? 1 : 0;
        } finally {
            RWLock.readLock().unlock();
        }
    }


    private void ChangeMode() {
        RWLock.readLock().lock();
        try {
            if (!forward) {
                step *= -1;
            }
        } finally {
            RWLock.readLock().unlock();
        }
    }

    private int getStep() {
        RWLock.readLock().lock();
        try {
            return step;
        } finally {
            RWLock.readLock().unlock();
        }
    }

    private int NextProcessingID() {
        RWLock.readLock().lock();

        try {
            int result = ProcessingID[Mode()] + getStep();
            if (result < 1) {
                ChangeMode();
                result = ProcessingID[Mode()] + getStep();
            }
            return result;
        } finally {
            RWLock.readLock().unlock();
        }
    }

    private Map<Integer, Map<Integer, Integer>> cache_rate_map;

    private Map<Integer, Integer> FindCacheRange(int first_id) {
        if (cache_rate_map == null) {
            return null;
        }
        return cache_rate_map.get(first_id);
    }

    private final int[] ProcessingID = new int[2];
    private int step = -1;
    private int MaxNo404 = 0;
    private int Count404 = 0;


    private void RefreshMaxNo404(int id) {
        RWLock.writeLock().lock();
        try {
            if (id > MaxNo404) {
                MaxNo404 = id;
                Count404 = 0;

                try (FileWriter writer = new FileWriter("MaxPixivID.txt")) {
                    writer.write(MaxNo404 + "");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } finally {
            RWLock.writeLock().unlock();
        }

    }

    private void AddCount404() {
        RWLock.writeLock().lock();
        try {
            Count404++;
        } finally {
            RWLock.writeLock().unlock();
        }
    }


    private int PollProcessingID() {
        RWLock.writeLock().lock();
        try {

            while (true) {
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    break;
                }
                int first_id = ProcessingID[Mode()] / 1000;
                int last_id = ProcessingID[Mode()] % 1000;
                Map<Integer, Integer> range = FindCacheRange(first_id);
                if (range != null) {

                    Integer r;
                    if (!forward && (r = range.get(last_id)) != null && r <= 5100) {
                        System.out.println("PixivID:" + ProcessingID[Mode()] + " (PollSkip:lowChRate)");
                        time_skip++;
                        ProcessingID[Mode()] += getStep();
                    } else {
                        break;
                    }

                } else {

                    System.out.println("Refresh Rate Cache");
                    Map<Integer, Map<Integer, Integer>> new_map = jedis.call(new JedisCaller<Map<Integer, Map<Integer, Integer>>>() {
                        @Override
                        public Map<Integer, Map<Integer, Integer>> call(Jedis jedis) {
                            String pre_pattern;
                            String pattern = "*";
                            if (100 > first_id && first_id > -100) {
                                if (10 > first_id && first_id > -10) {
                                    pattern = "";
                                }
                                pre_pattern = String.valueOf(first_id);
                            } else {
                                pre_pattern = String.valueOf(first_id / 100);
                            }
                            Map<Integer, Map<Integer, Integer>> new_cache_map = new HashMap<>();
                            Set<String> keys = jedis.keys(pre_pattern + pattern);
                            for (String key : keys) {
                                Map<String, String> map = jedis.hgetAll(key);
                                int fk = Integer.valueOf(key);
                                new_cache_map.put(fk, new HashMap<>());
                                for (String k : map.keySet()) {
                                    new_cache_map.get(fk).put(Integer.parseInt(k), Integer.valueOf(map.get(k)));
                                }
                            }
                            return new_cache_map;
                        }
                    });
                    int cache_size = 0;

                    if (cache_rate_map == null) {
                        cache_rate_map = new_map;
                    } else {
                        cache_rate_map.putAll(new_map);

                        for (Map<Integer, Integer> m : cache_rate_map.values()) {
                            cache_size += m.size();
                        }
                        System.out.println(cache_size);
                        if (cache_size > 500000) {
                            cache_rate_map = new_map;
                            System.out.println("Clear Cache Map");
                        }
                    }


                    if (FindCacheRange(first_id) == null) {
                        break;
                    }

                }
            }
            int id = ProcessingID[Mode()];
            ProcessingID[Mode()] += getStep();
            if (Count404 < 10000 || Mode() == 1) {
                ChangeMode();
            }
            if (forward) {
                if (Count404 > 10000) {
                    Count404 = 0;
                    ProcessingID[Mode()] = MaxNo404;
                    System.out.println("ForwardPoll:Sleep");
                    try {
                        Thread.sleep(10 * 60 * 1000);
                    } catch (InterruptedException ignored) {

                    }
                }

            }

            return id;

//            while (ch_rate_cache != null && ch_rate_cache.contains(ProcessingID)) {
//                System.out.println("PixivID:" + ProcessingID + " (skip)");
//                time_skip++;
//                ProcessingID--;
//            }
//            int id = ProcessingID;
//            ProcessingID--;
//            return id;
        } finally {
            RWLock.writeLock().unlock();
        }
    }

    private static boolean isRunning = false;

    private static ThreadPoolExecutor executor;

    private static Lock StaticLock = new ReentrantLock(true);

    public static void stop() {
        StaticLock.lock();
        try {
            if (executor != null && !executor.isShutdown()) {
                executor.shutdownNow();
            }

            while (executor != null && !executor.isShutdown()) {
            }
            isRunning = false;
        } finally {
            StaticLock.unlock();
        }
    }

    public ConcurrentSkipListSet<Integer> ch_rate_cache;

    public AutoCloseDataSource dataSource;

    private static final String db_name = "pixiv.db";

    public boolean InitDB() {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            e.printStackTrace(System.out);
            return false;
        }
        String sql_table_list = "SELECT name FROM sqlite_master WHERE type='table'";
        try (Connection conn = DriverManager.getConnection(JDBC.PREFIX + db_name);
             Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql_table_list)) {


            List<String> table_list = new ArrayList<String>();
            while (rs.next()) {
                table_list.add(rs.getString("name"));
            }

            if (dataSource == null) {
                dataSource = new AutoCloseDataSource(new SQLiteDataSource() {{
                    setUrl(JDBC.PREFIX + db_name);
                }});
            }

            for (int i = 0; i < table_list.size(); i++) {
                String name = table_list.get(i);
                if (name != null) {
                    table_list.set(i, name.toLowerCase());
                }
            }

            if (!table_list.contains("pixiv_novel_info")) {
                String sql_create_table = "CREATE TABLE pixiv_novel_info(" +
                        "'id' INT(10)" + "," +
                        "'title' VARCHAR(255)" + "," +
                        "'user_id' INT(10)" + "," +
                        "'cover_type' VARCHAR(255)" + "," +
                        "'cover_id' INT(10)" + "," +
                        "'serialized_value' VARCHAR(255)" + "," +
                        "'tag' VARCHAR(255)" + "," +
                        "'comment' VARCHAR(255)" + "," +
                        "'scene' INT(10)" + "," +
                        "'restrict' INT(10)" + "," +
                        "'count'  INT(10)" + "," +
                        "'score' INT(10)" + "," +
                        "'view' INT(10)" + "," +
                        "'novel_cover_preset_id' INT(10)" + "," +
                        "'novel_cover_img_name' VARCHAR(255)" + "," +
                        "'novel_cover_img_ext' VARCHAR(255)" + "," +
                        "'type' VARCHAR(255)" + "," +
                        "'url' VARCHAR(255)" + "," +
                        "'one_comment_count' INT(10)" + "," +
                        "'bookmark_count' INT(10)" + "," +
                        "'user_name' VARCHAR(255)" + "," +
                        "'marker_count' INT(10)" + "," +
                        "'series_id' INT(10)" + "," +
                        "'series_title' VARCHAR(255)" + "," +
                        "'text_length' INT(10)" + "," +
                        "'reupload_date' INT(10)" + "," +
                        "'upload_time' INT(10)" + "," +
                        "'r18' INT(10)" + "," +
                        "PRIMARY KEY('id')" +
                        ")";
                dataSource.update(sql_create_table);
            }

            return true;

        } catch (SQLException e) {
            e.printStackTrace(System.out);
            return false;

        }
    }

    public static String buildUrl(int PixivID) {
        return "https://www.pixiv.net/novel/show.php?id=" + PixivID + "&mode=text";
    }


    public static boolean start(boolean useProxy, boolean forward, int start_id) {

        StaticLock.lock();
        try {
            if (isRunning) {
                return false;
            }


            try {
                UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
            } catch (ClassNotFoundException | InstantiationException | UnsupportedLookAndFeelException | IllegalAccessException e) {
                e.printStackTrace(System.out);
            }


//            executor = Executors.newFixedThreadPool(192);
            int nThreads = 192;
            executor = new ThreadPoolExecutor(nThreads, nThreads,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>());
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try (FileWriter rate = new FileWriter("rate.txt"); FileWriter speed = new FileWriter("speed.txt")) {
                        rate.write("");
                        speed.write("");
                    } catch (IOException e) {
                        e.printStackTrace(System.out);
                    }

                    pixiv p = new pixiv(useProxy, forward);


                    if (!p.InitDB()) {
                        executor.shutdownNow();
                        return;
                    }

                    if (useProxy) {
                        p.getProxyPool().start();
                    }
                    p.time = System.currentTimeMillis();

                    if (forward) {
                        String max_id_sql = "SELECT MAX(id) AS MaxID FROM pixiv_novel_info";
                        int max_id = p.dataSource.query(new DataSourceCallBack<Integer>() {
                            @Override
                            public Integer callback(ResultSet rs) {
                                try {
                                    if (rs.next()) {
                                        return rs.getInt("MaxID");
                                    }
                                } catch (SQLException e) {
                                    ExceptionUtil.printSingleStackTrace(e);
                                }
                                return 0;
                            }
                        }, max_id_sql);
                        p.ProcessingID[0] = max_id;
                        p.ProcessingID[1] = max_id + 1;
                    } else {
                        p.ProcessingID[0] = 9641223;
                        p.ProcessingID[1] = 9641223 + 1;
                    }


                    if (start_id > 0) {
                        p.ProcessingID[0] = start_id;
                        p.ProcessingID[1] = start_id + 1;
                    }

                    Set<String> ch_rate = p.jedis.call(new JedisCaller<Set<String>>() {
                        @Override
                        public Set<String> call(Jedis jedis) {
//                        long count = jedis.zcard("pixiv.novel:ch_rate");
                            return jedis.zrevrangeByScore("pixiv.novel:ch_rate", 0.51d, -100d);
//                            return jedis.zrangeWithScores("pixiv.novel:ch_rate", 0, count);
                        }
                    });

                    if (ch_rate != null) {
                        p.ch_rate_cache = new ConcurrentSkipListSet<>();
                        for (String id : ch_rate) {
                            p.ch_rate_cache.add(Integer.parseInt(id));
                        }
                    }


//                    ProcessingID = 9640465;
                    while (p.NextProcessingID() > 0) {
//                        if (executor.getQueue().size() > 500) {
//                            System.out.println("Wait");
//                            while (executor.getQueue().size() > 10) {
//                                try {
//                                    Thread.sleep(100);
//                                } catch (InterruptedException ignored) {
//                                }
//                            }
//                            System.out.println("Resume");
//                        }

                        if (executor.getQueue().size() > 10) {
                            System.out.println("Wait");
                            while (executor.getActiveCount() > executor.getPoolSize() - 50) {
                                try {
                                    Thread.sleep(100);
                                } catch (InterruptedException ignored) {
                                }
                            }
                            System.out.println("Resume");
                        }

                        executor.execute(new Runnable() {

                            @Override
                            public void run() {
                                boolean skip_flag = false;
                                do {


                                    int id = p.PollProcessingID();
                                    Double rate = null;
                                    String path = null;
                                    Boolean exists = null;
                                    Boolean dataExists = null;


                                    if (!forward) {
                                        rate = p.jedis.call(new JedisCaller<Double>() {
                                            @Override
                                            public Double call(Jedis jedis) {


                                                String r = jedis.hget(String.valueOf(id / 1000), String.valueOf(id % 1000));
                                                if (r != null) {
                                                    return Double.parseDouble(r) / 10000d;
                                                } else {
                                                    return null;
                                                }

                                            }
                                        });

                                        if (rate != null && !(rate > 0.51d)) {
                                            System.out.println("PixivID:" + id + " (skip:lowChRate)");
                                            skip_flag = true;
                                            break;
                                        }
                                    }

                                    String sql_find_info = "SELECT id FROM pixiv_novel_info WHERE id='" + id + "'";

                                    dataExists = p.dataSource.query(new DataSourceCallBack<Boolean>() {
                                        @Override
                                        public Boolean callback(ResultSet rs) {


                                            try {
                                                return rs.next();
                                            } catch (SQLException e) {
                                                e.printStackTrace(System.out);
                                                return false;
                                            }

                                        }
                                    }, sql_find_info);


//                                    dataExists = p.jedis.call(new JedisCaller<Boolean>() {
//                                                @Override
//                                                public Boolean call(Jedis jedis) {
//
//                                                    return jedis.hexists("pixiv.novel.info:" + id, "id");
//                                                }
//                                            })

                                    if (dataExists != null && dataExists) {
                                        File f = new File(TextStoragePath, id + ".txt");
                                        path = f.getAbsolutePath();
                                        exists = f.exists();
                                        if (f.exists()) {
                                            System.out.println("PixivID:" + id + " (skip:fileExists)");
                                            skip_flag = true;
                                            break;
                                        }
                                    }


                                    if (p.lock.tryLock())

                                    {
                                        try {
                                            if (p.console != null) {
                                                p.console.ClearAndPrint("Speed:" + p.Speed() + "/s");
                                                p.console.println("PixivID:" + id);
                                            } else {
                                                System.out.println("PixivID:" + id + "    [ch_rate=" + rate + ",dataExists=" + dataExists + ",fileExists=" + exists + ",path=" + path + "]");
                                            }
                                        } finally {
                                            p.lock.unlock();
                                        }
                                    }
                                    p.parseOne(buildUrl(id));


                                } while (false);
                                while (!p.lock.tryLock()) {
                                    System.out.println(Thread.currentThread().getName() + ":tryLock");
                                    try {
                                        Thread.sleep(100);
                                    } catch (InterruptedException ignored) {

                                    }
                                }
                                double parse_speed = 0;
                                double skip_speed = 0;
                                boolean speed_flag = false;
                                try {
                                    p.count++;
                                    if (skip_flag) {
                                        p.time_skip++;
                                    } else {
                                        p.time_count++;
                                    }

                                    long timestamp = System.currentTimeMillis();
                                    if (timestamp - p.time > 500) {
                                        speed_flag = true;
                                        long dur = timestamp - p.time;
                                        parse_speed = Math.round(
                                                (double) p.time_count / ((double) dur / 1000d) * 100d
                                        ) / 100d;
                                        p.Speed(parse_speed);

                                        skip_speed = Math.round(
                                                (double) p.time_skip / ((double) dur / 1000d) * 100d
                                        ) / 100d;

                                        p.time_count = 0;
                                        p.time_skip = 0;
                                        p.time = timestamp;
                                    }

                                } finally {
                                    p.lock.unlock();
                                }

                                if (speed_flag && p.lock.tryLock()) {
                                    try (FileWriter writer = new FileWriter("speed.txt", true)) {

                                        writer.write("Speed:" + Math.round((parse_speed + skip_speed) * 100d) / 100d + "/s ParseSpeed:" + parse_speed + "/s SkipSpeed:" + skip_speed + "/s\n");

                                    } catch (IOException e) {
                                        e.printStackTrace(System.out);
                                    } finally {
                                        p.lock.unlock();
                                    }
                                }

                            }
                        });
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException ignored) {

                        }
                    }
                }
            });
            if (!executor.isShutdown()) {
                isRunning = true;
                return true;
            } else {
                return false;
            }
        } finally {
            StaticLock.unlock();
        }

    }

    private static int _count = 0;

    public static void main(String[] args) throws IOException, SQLException {
        if (false) {

//            System.out.println(JSONObject.fromObject(map).toString(1));

            Connection connection = DriverManager.getConnection("jdbc:mysql://67.218.158.124:3306/crawler?autoReconnect=true&useUnicode=true&characterEncoding=utf8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true", "xwv", "OmegaX!!");

            Map<String, Map<String, String>> map = ParseUserNovelList(25171717, Proxy.NULL);
            String update_sql = CreateInfoPreparedUpdateSql();
            PreparedStatement s = connection.prepareStatement(update_sql);
            for (Map<String, String> m : map.values()) {
                SetInfoUpdatePreparedParams(s, m);
                s.execute();
            }

            s.execute();
            s.close();
            connection.close();
//            System.out.println(sql);
            return;
        }


//        if (false) {
//            for (int i = 0; i < 100 * 1000; i++)
//                executor.execute(new Runnable() {
//                    @Override
//                    public void run() {
//                        post("https://www.pixiv.net/novel/show.php?id=9623601&mode=text", getHeaders());
//
//                    }
//                });
//            return;
//        }

//        if (false) {
//            System.out.println("===");
//            String text = "[newpage] \n\n [newpage] \n aaa\n [newpage]   \n";
//
//            System.out.println(text);
//            System.out.println("===");
//            System.out.println(text.replaceAll(".*\\[newpage].*[\\n]?", ""));
//            System.out.println("===");
//            return;
//        }


//        while (p.getProxyPool().getNext() == null)
//        Proxy proxy = null;
//        while (proxy == null) {
//            proxy = p.getProxyPool().getNext();
//        }
//        p.getProxyPool().stop();
//        if (false) {
//            pixiv p = new pixiv();
//            p.getProxyPool().start();
//            p.parseOne("https://www.pixiv.net/novel/show.php?id=8285786&mode=text");
//            p.getProxyPool().stop();
//
//            return;
//        }
//        System.out.println(proxy.Post("https://www.pixiv.net", cookie));

//        client.dispatcher().setMaxRequests(128);
//        if (false) {
//            String[] a = "aaa".split(newPageRegex);
//            return;
//        }

        start(false, false, 0);

//        p.getProxyPool().stop();


    }


//    private static OkHttpClient client = new OkHttpClient.Builder()
//            .connectTimeout(10 * 1000, TimeUnit.MILLISECONDS)
//            .readTimeout(20, TimeUnit.SECONDS)
//            .connectionPool(new ConnectionPool(1024, 5 * 1000, TimeUnit.MILLISECONDS))
//            .dns(dns)
//            .build();

//    private static final Lock testlock = new ReentrantLock(true);


//    public static Response OkHttpPost(String url, Headers headers) {
//
//        testlock.lock();
//        testlock.unlock();
//        OkHttpClient client = NullProxy.getHttpClient();
//        Request.Builder builder = new Request.Builder()
//                .method("GET", null)
////                    .header("Connection", "close")
//                .url(url);
//        if (headers != null) {
//            builder.headers(headers);
//        }
//        Request request = builder.build();
//        try (okhttp3.Response response = client.newCall(request).execute(); ResponseBody body = response.body()) {
//            xwv.bean.Response resp;
//            String resp_body = body != null ? GzipUtil.decompress(body.byteStream()) : null;
//            boolean isGzip = false;
//            if (resp_body == null) {
//                resp = new xwv.bean.Response(response.code(), body != null ? body.string() : null);
//            } else {
//                resp = new xwv.bean.Response(response.code(), resp_body);
//                isGzip = true;
//            }
//
//            if (response.code() < 500) {
//                return resp;
//            }
//
//        } catch (Exception e) {
//            System.out.println(e.toString());
//        }
//
//        testlock.lock();
//        testlock.unlock();
//        return null;
//    }

    public static Response post(String url, Headers headers) {
        try {
            HttpsURLConnection connection = (HttpsURLConnection) new URL(url).openConnection();
            for (String name : headers.names()) {

                connection.setRequestProperty(name, headers.get(name));

            }
//            connection.setRequestProperty("connection", "close");
            connection.setConnectTimeout(3 * 1000);
            connection.setReadTimeout(5 * 1000);
            connection.setRequestMethod("GET");

            long timestamp = System.currentTimeMillis();
            connection.connect();
//            long now = System.currentTimeMillis();

            int code = connection.getResponseCode();
//            System.out.println("connect:" + (System.currentTimeMillis() - timestamp));
            try (InputStream input = code == 200 ? connection.getInputStream() : connection.getErrorStream();
                 BufferedInputStream bis = new BufferedInputStream(input);
                 ByteArrayOutputStream bos = new ByteArrayOutputStream()) {

                byte[] buffer = new byte[1024];
                int len;
                while ((len = bis.read(buffer)) > -1) {
                    bos.write(buffer, 0, len);
                }
                bos.flush();
                byte[] bytes = bos.toByteArray();


//            System.out.println(System.currentTimeMillis() - timestamp);
                String body = GzipUtil.decompress(new ByteArrayInputStream(bytes));
                if (body == null) {
                    body = new String(bytes, "UTF-8");
                }

                if (code < 500) {
                    return new Response(code, body);
                }
            } finally {
                connection.disconnect();
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        return null;
    }


}
