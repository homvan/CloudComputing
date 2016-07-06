import java.io.*;
import java.util.Scanner;

/**
 * Created by ken on 2015/10/28.
 */
public class Q6Classifier {

    public static void main(String[] args) {
        BufferedReader br = null;
        Writer[] fis = new Writer[9];
        try {
            br = new BufferedReader(new InputStreamReader(System.in, "UTF8"));
            for (int i = 0; i < 9; i++) {
                fis[i] = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream("q6_" + i+".txt"), "utf-8"));
            }
        }catch (FileNotFoundException ex) {

        }catch (UnsupportedEncodingException ex2) {

        }
        String data;
        try {
            while ((data = br.readLine()) != null) {
                String[] keys = data.split("\t");
                if (keys.length != 2) {
                    System.out.println(data);
                    break;
                }
                int cata = 0;
                try {
                    cata = Integer.parseInt(keys[0].substring(keys[0].length() - 2, keys[0].length())) % 9;
                } catch (NumberFormatException ex) {
                    System.out.println(data);
                    break;
                }
                Writer ww = fis[cata];
                ww.write(data + "\n");
            }
        }catch (IOException ex) {

        }
        for (Writer ww: fis) {
            try {
                ww.close();
            }catch (IOException ex) {

            }
        }
    }
}
