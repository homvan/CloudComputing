
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

/**
 * Created by ken on 2015/10/27.
 */
public class DataParser6 {


    private HashSet<String> censored;

    public static void main(String[] args) {
        DataParser6 dp = new DataParser6();
        dp.initialCensored();
        dp.go();
    }

    public void go() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in, "UTF8"));
            String input = null;
            JsonParser jp = new JsonParser();
            while ((input = br.readLine()) != null) {
                JsonElement je = jp.parse(input);
                JsonObject jo = je.getAsJsonObject();
                String id = jo.get("id").toString();
                String text = jo.get("text").toString();
                text = text.substring(1, text.length()-1);
                String content = ScoreAndCensor(text);
                String result = (id + "\t" + content);
                PrintStream out = new PrintStream(System.out, true, "UTF-8");
                out.println(result);
            }
        } catch (IOException ex) {

        }
    }

    private String ScoreAndCensor(String str) {
        String result = "";
        for (int i = 0; i < str.length(); ) {
            int head = i;
            int tail = i;
            char c = str.charAt(head);
            if (head != 0 && isSpecial(c)) {
                if (str.charAt(head -1) == '\\') {
                    i = head + 1;
                    result += c;
                    continue;
                }
            }
            while (isEnglishOrNum(c)) {
                tail += 1;
                if (tail == str.length())
                    break;
                c = str.charAt(tail);
            }
            if (head == tail)
                tail += 1;
            String temp = str.substring(head, tail);
            if (censored.contains(temp.toLowerCase())) {
                String censore = "";
                for (int m = 0; m < temp.length(); m++) {
                    if (m == 0)
                        censore += temp.charAt(m);
                    else if (m == temp.length() - 1)
                        censore += temp.charAt(m);
                    else
                        censore += "*";
                }
                result += censore;
            } else {
                result += temp;
            }
            i = tail;
        }
        return result;
    }

    private String rot13(String str) {
        String result = "";
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (Character.isUpperCase(c)) {
                c += 13;
                if (c > 'Z')
                    c -= 26;
            }
            if (Character.isLowerCase(c)) {
                c += 13;
                if (c > 'z')
                    c -= 26;
            }
            result += c;
        }
        return result;
    }

    private void initialCensored() {
        censored = new HashSet<String>();
        FileInputStream in = null;
        try {
            in = new FileInputStream("./Banned.txt");
            Scanner scanner = new Scanner(in);
            String data;
            while (scanner.hasNext()) {
                data = scanner.nextLine();
                censored.add(rot13(data).toLowerCase());
            }
            in.close();
        } catch (IOException ex) {

        }
    }

    private boolean isEnglishOrNum(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
    }

    private boolean isSpecial(char c) {
        return (c == 'n' || c == 'r' || c == 't' || c == 'b');
    }
}
