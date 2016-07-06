import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;

/**
 * Created by ken on 2015/11/23.
 */
public class DataParser5 {
    public static void main(String[] args) {
        DataParser5 dp = new DataParser5();
        dp.go();
    }
    private void go() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in, "UTF8"));
            String input = null;
            JsonParser jp = new JsonParser();
            PrintStream out = new PrintStream(System.out, true, "UTF-8");
            while ((input = br.readLine()) != null) {
                try {
                    JsonElement je = jp.parse(input);
                    JsonObject jo = je.getAsJsonObject();
                    JsonElement user = jo.get("user");
                    String user_id = user.getAsJsonObject().get("id").toString();
                    String tid = jo.get("id").toString();
                    out.println(user_id +  "\t" + tid);
                }catch (Exception ex2) {
                    System.out.println(ex2);
                    continue;
                }
            }
            br.close();
        } catch (IOException ex) {

        }
    }
}
