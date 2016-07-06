import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by ken on 2015/11/23.
 */
public class Q5Reducer {
    public static void main(String[] args) {
        Q5Reducer qr = new Q5Reducer();
        qr.go();
    }

    private void go() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in, "UTF8"));
            String prevUser = "";
            String currUser = "";
            String data;
            HashSet<String> mySet = new HashSet<String>();
            PrintStream out = new PrintStream(System.out, true, "UTF-8");
            while ((data = br.readLine()) != null) {
                String[] temps = data.split("\t");
                currUser = temps[0];
                if (!currUser.equals(prevUser) && !prevUser.equals("")) {
                    out.println(prevUser + "\t" + mySet.size());
                    mySet.clear();
                }
                mySet.add(temps[2]);
                prevUser = currUser;
            }
            out.println(prevUser + "\t" + mySet.size());
            br.close();
        } catch (IOException ex) {

        }
    }
}
