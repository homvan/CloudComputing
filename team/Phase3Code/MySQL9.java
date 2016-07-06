import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;

import java.math.BigInteger;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by ken on 2015/11/11.
 */
public class MySQL9 extends AbstractVerticle {

    private ArrayList<String> dcs = new ArrayList<String>(9);
    private ArrayList<Connection> conns = new ArrayList<Connection>(9);
    private static final String TEAMINFO = "AmazonCEO,672111576288\n";
    private static final String SQL_DNS_1 = "jdbc:mysql://ec2-52-90-214-235.compute-1.amazonaws.com:3306/team";
    private static final String SQL_DNS_2 = "jdbc:mysql://ec2-52-90-205-127.compute-1.amazonaws.com:3306/team";
    private static final String SQL_DNS_3 = "jdbc:mysql://ec2-52-90-226-111.compute-1.amazonaws.com:3306/team";
    private static final String SQL_DNS_4 = "jdbc:mysql://ec2-54-86-229-139.compute-1.amazonaws.com:3306/team";
    private static final String SQL_DNS_5 = "jdbc:mysql://ec2-54-86-220-10.compute-1.amazonaws.com:3306/team";
    private static final String SQL_DNS_6 = "jdbc:mysql://ec2-54-86-235-171.compute-1.amazonaws.com:3306/team";
    private static final String SQL_DNS_7 = "jdbc:mysql://ec2-52-90-240-141.compute-1.amazonaws.com:3306/team";
    private static final String SQL_DNS_8 = "jdbc:mysql://ec2-54-86-114-214.compute-1.amazonaws.com:3306/team";
    private static final String SQL_DNS_9 = "jdbc:mysql://localhost:3306/team";
    private Connection self;

    //q1
    private static BigInteger x = new BigInteger("8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773");
    private static String TEAMINFO_1 = "AmazonCEO,6721-1157-6288";
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static BigInteger offset = new BigInteger("25");
    //q2
    private static final String QUERY_PREFIX_2 = "Select content From q2 WHERE mykey='";
    //q3
    private static final String QUERY_PREFIX_POSITIVE = "Select content From q3P WHERE id=";
    private static final String QUERY_PREFIX_NEGATIVE = "Select content From q3N WHERE id=";
    private static final String POSITIVE_TWEET = "Positive Tweets\n";
    private static final String NEGATIVE_TWEET = "\nNegative Tweets\n";
    private static final String ADB = " and date BETWEEN ";
    private static final String PORDER = " order by score desc, content ASC limit ";
    private static final String NORDER = " order by score, content ASC limit ";
    //q4
    private static final String QUERY_PREFIX_4 = "Select content From q4 WHERE hashtag='";
    private static final String ORDER = " order by tagcount desc, date ASC limit ";
    //q5
    private static final String QUERY_PREFIX = "(Select id From q5 WHERE id";
    private static final String QUERY_SUFFIX = " order by id desc limit 1)";
    private static final String FINAL_Q1 = "select ((select number from q5 where id=";
    private static final String FINAL_Q2 = ") - (select number from q5 where id=";
    private static final String FINAL_Q3 = ")) as diff;";

    //q6
    private HashMap<Long, Helper> myMap;
    private int selfID;
    private ArrayList<String> dnsS;
    private static final String Q6_QUERY = "select content from q6 where mykey=";
    private static final String DNS_1 = "ec2-52-90-214-235.compute-1.amazonaws.com";
    private static final String DNS_2 = "ec2-52-90-205-127.compute-1.amazonaws.com";
    private static final String DNS_3 = "ec2-52-90-226-111.compute-1.amazonaws.com";
    private static final String DNS_4 = "ec2-54-86-229-139.compute-1.amazonaws.com";
    private static final String DNS_5 = "ec2-54-86-220-10.compute-1.amazonaws.com";
    private static final String DNS_6 = "ec2-54-86-235-171.compute-1.amazonaws.com";
    private static final String DNS_7 = "ec2-52-90-240-141.compute-1.amazonaws.com";
    private static final String DNS_8 = "ec2-54-86-114-214.compute-1.amazonaws.com";
    private static final String DNS_9 = "ec2-54-86-230-117.compute-1.amazonaws.com";
    @Override
    public void start() {
        HttpServer server = vertx.createHttpServer();
        selfID = 8;
        myMap = new HashMap<Long, Helper>();
        dnsS = new ArrayList<String>(9);
        dcs.add(SQL_DNS_1);
        dcs.add(SQL_DNS_2);
        dcs.add(SQL_DNS_3);
        dcs.add(SQL_DNS_4);
        dcs.add(SQL_DNS_5);
        dcs.add(SQL_DNS_6);
        dcs.add(SQL_DNS_7);
        dcs.add(SQL_DNS_8);
        dcs.add(SQL_DNS_9);
        dnsS.add(DNS_1);
        dnsS.add(DNS_2);
        dnsS.add(DNS_3);
        dnsS.add(DNS_4);
        dnsS.add(DNS_5);
        dnsS.add(DNS_6);
        dnsS.add(DNS_7);
        dnsS.add(DNS_8);
        dnsS.add(DNS_9);


        try {
            Class.forName("com.mysql.jdbc.Driver");
            for (int i = 0; i < 9; i++) {
                Connection conn = DriverManager.getConnection(dcs.get(i), "team", "123");
                conns.add(conn);
            }
            //======================================================================
            self = conns.get(8); //change number here!!!!!
            //======================================================================
            HttpClient httpClient = vertx.createHttpClient();
            server.requestHandler(httpServerRequest -> {
                String uri = httpServerRequest.uri();
                try {
                    if (uri.startsWith("/q1")) {
                        q1(httpServerRequest);
                    } else if (uri.startsWith("/q2")) {
                        q2(httpServerRequest);
                    } else if (uri.startsWith("/q3")) {
                        q3(httpServerRequest);
                    } else if (uri.startsWith("/q4")) {
                        q4(httpServerRequest);
                    } else if (uri.startsWith("/q5")) {
                        q5(httpServerRequest);
                    }else if (uri.startsWith("/q6")) {
                        q6(httpServerRequest, httpClient);
                    }else if (uri.startsWith("/a")) {
                        httpServerRequest.response().end();
                        doAppend(httpServerRequest);
                    }else if (uri.startsWith("/r")) {
                        doRead(httpServerRequest);
                    }else if (uri.startsWith("/e")) {
                        httpServerRequest.response().end();
                        doEnd(httpServerRequest);
                    }else {
                        httpServerRequest.response().end("good");
                    }
                } catch (SQLException ex) {

                }
            }).listen(8080);

        } catch (ClassNotFoundException ex) {
            System.out.println(ex);
        } catch (SQLException ex2) {
            System.out.println(ex2);
        }
    }

    private void q1(HttpServerRequest httpServerRequest) {
        String key = httpServerRequest.getParam("key");
        String message = httpServerRequest.getParam("message");
        BigInteger y = new BigInteger(key).divide(x);
        int shift = y.mod(offset).intValue() + 1;
        String temp_result = decode(message);
        StringBuilder realMessage = new StringBuilder();
        for (int i = 0; i < temp_result.length(); i++) {
            int temp = temp_result.charAt(i) - shift - 'A';
            if (temp >= 0)
                realMessage.append((char) (temp + 'A'));
            else
                realMessage.append((char) (temp + 'Z' + 1));
        }
        StringBuilder result = new StringBuilder(TEAMINFO_1)
                .append("\n")
                .append(sdf.format(new java.util.Date()))
                .append("\n")
                .append(realMessage.toString())
                .append("\n");
        httpServerRequest.response().end(result.toString());
    }

    private void q2(HttpServerRequest httpServerRequest) throws SQLException {
        String userID = httpServerRequest.getParam("userid");
        String tweetTime = httpServerRequest.getParam("tweet_time");
        String simplifiedTime = tweetTime.substring(8, 19).replaceAll(":", "").replaceAll(" ", "");
        StringBuilder qq = new StringBuilder(QUERY_PREFIX_2).append(userID).append(simplifiedTime).append("';");
        Connection conn = conns.get(Integer.parseInt(tweetTime.substring(14, 16)) % 9);
        Statement state = conn.createStatement();
        ResultSet rs = state.executeQuery(qq.toString());
        if (rs.next()) {
            String reply = new StringBuilder(TEAMINFO).append(rs.getString("content")).append("\n").toString();
            httpServerRequest.response().end(reply);
            state.close();
            rs.close();
        }else
            httpServerRequest.response().end("");


    }

    private void q3(HttpServerRequest httpServerRequest) throws SQLException {
        String sDate = httpServerRequest.getParam("start_date");
        String eDate = httpServerRequest.getParam("end_date");
        String n = httpServerRequest.getParam("n");
        String userID = httpServerRequest.getParam("userid");
        String start = sDate.substring(2, 10).replaceAll("-", "");
        String end = eDate.substring(2, 10).replaceAll("-", "");
        StringBuilder foot = new StringBuilder(ADB).append(start).append(" and ").append(end);
        StringBuilder pQ = new StringBuilder(QUERY_PREFIX_POSITIVE).append(userID)
                .append(foot).append(PORDER).append(n).append(";");
        StringBuilder nQ = new StringBuilder(QUERY_PREFIX_NEGATIVE)
                .append(userID).append(foot).append(NORDER).append(n).append(";");
        int length = userID.length();
        Connection conn = conns.get(Integer.parseInt(userID.substring(length - 2, length)) % 9);
        Statement state = conn.createStatement();
        ResultSet pRS = state.executeQuery(pQ.toString());
        StringBuilder result = new StringBuilder(TEAMINFO);
        result.append(POSITIVE_TWEET);
        while (pRS.next()) {
            result.append(pRS.getString("content")).append("\n");
        }
        pRS.close();
        ResultSet nRS = state.executeQuery(nQ.toString());
        result.append(NEGATIVE_TWEET);
        while (nRS.next()) {
            result.append(nRS.getString("content")).append("\n");
        }
        nRS.close();
        httpServerRequest.response().end(result.toString());
        state.close();
    }

    private void q4(HttpServerRequest httpServerRequest) throws SQLException {
        String hashtag = httpServerRequest.getParam("hashtag");
        String n = httpServerRequest.getParam("n");
        StringBuilder qq = new StringBuilder(QUERY_PREFIX_4).append(hashtag).append("'").append(" COLLATE 'utf8mb4_bin'")
                .append(ORDER).append(n).append(";");
        Statement state = self.createStatement();
        ResultSet rs = state.executeQuery(qq.toString());
        StringBuilder result = new StringBuilder(TEAMINFO);
        while (rs.next()) {
            result.append(rs.getString("content")).append("\n");
        }
        rs.close();
        httpServerRequest.response().end(result.toString());
        state.close();
    }

    private void q5(HttpServerRequest httpServerRequest) throws SQLException {
        String min = httpServerRequest.getParam("userid_min");
        String max = httpServerRequest.getParam("userid_max");
        StringBuilder from = new StringBuilder(QUERY_PREFIX).append("<").append(min).append(QUERY_SUFFIX);
        StringBuilder to = new StringBuilder(QUERY_PREFIX).append("<=").append(max).append(QUERY_SUFFIX);
        Statement state = self.createStatement();
        StringBuilder result = new StringBuilder(FINAL_Q1).append(to).append(FINAL_Q2).append(from).append(FINAL_Q3);
        ResultSet rs = state.executeQuery(result.toString());
        if (rs.next())
            httpServerRequest.response().end(TEAMINFO+rs.getString("diff")+"\n");
        state.close();
    }

    private void q6(HttpServerRequest httpServerRequest, HttpClient httpClient) {
        String opt = httpServerRequest.getParam("opt");
        String tid = httpServerRequest.getParam("tid");
        String tag = httpServerRequest.getParam("tag");
        int length = tid.length();
        int cata = 0;
        if (length < 2)
            cata = Integer.parseInt(tid) % 9;
        else
            cata = Integer.parseInt(tid.substring(length - 2, length)) % 9;
        if (cata == selfID) {
            doResponse(httpServerRequest, opt, tag);
            if (opt.equals("a"))
                doAppend(httpServerRequest);
            else if (opt.equals("r"))
                doRead(httpServerRequest);
            else if (opt.equals("e"))
                doEnd(httpServerRequest);
        }else {
            doResponse(httpServerRequest, opt, tag);
            giveToOther(httpServerRequest, httpClient, cata, tid, opt, tag);
            return;
        }
    }


    private void doAppend(HttpServerRequest httpServerRequest) {
        Long tid = Long.parseLong(httpServerRequest.getParam("tid"));
        String seq = httpServerRequest.getParam("seq");
        String tweetid = httpServerRequest.getParam("tweetid");
        String tag = httpServerRequest.getParam("tag");
        if (!myMap.containsKey(tid))
            myMap.put(tid, new Helper());
        myMap.get(tid).add(new Operation(seq, tweetid, false, tag));
    }

    private void doRead(HttpServerRequest httpServerRequest) {
        Long tid = Long.parseLong(httpServerRequest.getParam("tid"));
        String seq = httpServerRequest.getParam("seq");
        String tweetid = httpServerRequest.getParam("tweetid");
        if (!myMap.containsKey(tid))
            myMap.put(tid, new Helper());
        myMap.get(tid).add(new Operation(seq, tweetid, true, httpServerRequest));
    }

    private void doEnd(HttpServerRequest httpServerRequest) {
        long tid = Long.parseLong(httpServerRequest.getParam("tid"));
        Thread tt = new Thread(new Runnable() {
            @Override
            public void run() {
                while(myMap.get(tid) == null) {
                    try {
                        Thread.sleep(1);
                    }catch (InterruptedException ex) {

                    }
                }
                while (myMap.get(tid).size() != 5) {
                    try {
                        Thread.sleep(1);
                    }catch (InterruptedException ex) {

                    }
                }
                Helper hp = myMap.get(tid);
                HashMap<String, String> tags = new HashMap<String, String>();
                for (int i = 0; i < 5; i++) {
                    int k = 0;
                    Operation op = null;
                    try {
                        op = hp.poll();
                    }catch (NullPointerException ex) {
                        continue;
                    }
                    if (op.isRead()) {
                        String latestTag = tags.get(op.getTweetid());
                        if (latestTag == null)
                            latestTag = "";
                        String tweetid = op.getTweetid();
                        int length = tweetid.length();
                        try {
                            Connection conn = conns.get(Integer.parseInt(tweetid.substring(length - 2, length)) % 9);
                            Statement state = conn.createStatement();
                            ResultSet rs = state.executeQuery(Q6_QUERY + tweetid + " ;");
                            if (rs.next())
                                op.getHttpServerRequest().response().end(TEAMINFO + rs.getString("content") + latestTag+"\n");
                            else
                                op.getHttpServerRequest().response().end("");
                            state.close();
                        }catch (SQLException ex) {

                        }
                    } else {
                        tags.put(op.getTweetid(), op.getTag());
                    }
                }
                myMap.remove(tid);
            }
        });
        tt.start();
    }

    private void giveToOther(HttpServerRequest httpServerRequest, HttpClient httpClient, int cata,
                             String tid, String opt, String tag){
        if (opt.equals("s"))
            return;
        StringBuilder url = new StringBuilder("/");
        if (opt.equals("e"))
            url.append(opt).append("?").append("tid=").append(tid);
        else {
            url.append(opt).append("?").append("tid=").append(tid)
                    .append("&opt=").append(opt)
                    .append("&seq=").append(httpServerRequest.getParam("seq"))
                    .append("&tweetid=").append(httpServerRequest.getParam("tweetid"));
            if (tag != null)
                url.append("&tag=").append(tag);
        }
        if (opt.equals("r")) {
            httpClient.getNow(80, dnsS.get(cata), url.toString(), new Handler<HttpClientResponse>() {
                @Override
                public void handle(HttpClientResponse httpClientResponse) {
                    httpClientResponse.bodyHandler(new Handler<Buffer>() {
                        @Override
                        public void handle(Buffer buffer) {
                            httpServerRequest.response().end(buffer.getString(0, buffer.length()));
                        }
                    });
                }
            });
        }else {
            httpClient.getNow(80, dnsS.get(cata), url.toString(), new Handler<HttpClientResponse>() {
                @Override
                public void handle(HttpClientResponse httpClientResponse) {

                }
            });
        }
    }

    private void doResponse(HttpServerRequest httpServerRequest, String opt, String tag) {
        if (opt.equals("s") || opt.equals("e")) {
            httpServerRequest.response().end(TEAMINFO + "0\n");
            return;
        } else if (opt.equals("a")) {
            httpServerRequest.response().end(TEAMINFO + tag+"\n");
            return;
        }
    }

    private class Helper {
        PriorityBlockingQueue<Operation> myQueue;
        public Helper() {
            myQueue = new PriorityBlockingQueue<Operation>(5);
        }

        public void add(Operation op) {
            myQueue.add(op);
        }

        public Operation poll() throws NullPointerException{
            return myQueue.poll();
        }

        public int size() {
            return myQueue.size();
        }

    }

    private class Operation implements Comparable<Operation> {
        private int seq;
        private String tweetid;
        private boolean isRead;
        private String tag;
        private HttpServerRequest httpServerRequest;

        public Operation(String seq, String tweetid, boolean opt, String tag) {
            this.seq = Integer.parseInt(seq);
            this.tweetid = tweetid;
            this.isRead = opt;
            this.tag = tag;
        }

        public Operation(String seq, String tweetid, boolean opt, HttpServerRequest httpServerRequest) {
            this.seq = Integer.parseInt(seq);
            this.tweetid = tweetid;
            this.isRead = opt;
            this.tag = "";
            this.httpServerRequest = httpServerRequest;
        }

        public String getTag() {
            return tag;
        }

        public boolean isRead() {
            return isRead;
        }

        public String getTweetid() {
            return tweetid;
        }

        public HttpServerRequest getHttpServerRequest() {
            return httpServerRequest;
        }

        @Override
        public int compareTo(Operation o) {
            return this.seq - o.seq;
        }
    }

    //Helper function
    private String decode(String message) {
        int squareLength = (int) Math.sqrt(message.length());
        StringBuilder chiper = new StringBuilder();
        for (int i = 0; i < squareLength; i++) {
            doAppendOne(i, chiper, message, squareLength);
        }
        for (int n = 2; n <= squareLength; n++) {
            doAppendOne(n * squareLength - 1, chiper, message, squareLength);
        }
        return chiper.toString();
    }

    private void doAppendOne(int index, StringBuilder chiper, String message, int squareLength) {
        // row
        int k = index / squareLength;
        // column
        int j = index % squareLength;
        while (k < squareLength && j >= 0) {
            chiper.append(message.charAt(j + squareLength * k));
            k++;
            j--;
        }
    }
}
