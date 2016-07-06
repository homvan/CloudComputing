import java.io.*;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Created by ken on 2015/10/31.
 */
public class q5Accumulator {

    public static void main(String[] args) {
        q5Accumulator qf = new q5Accumulator();
        qf.go();
    }

    private void go() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in, "UTF8"));
            int ii = 0;
            String data;
            while ((data = br.readLine()) != null) {
                String[] temps = data.split("\t");
                int temp = Integer.parseInt(temps[1]);
                ii += temp;
                PrintStream out = new PrintStream(System.out, true, "UTF-8");
                out.println(temps[0] + "\t" + ii);
            }
            br.close();
        }catch (IOException ex) {

        }
    }
}