import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

/**
 * Created by Bruce on 10/25/2017.
 */



public class FantasyBud {

    String user = "bruce145";
    String pass = "spdf7488";

    private void authenticate(){

        String cred = this.user + ":" + this.pass;

        try {

            URL url = new URL("");
            String encoding = Base64.getEncoder().encodeToString(cred.getBytes("UTF-8"));
            HttpURLConnection connection = (HttpURLConnection)url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty  ("Authorization", "Basic " + encoding);
            InputStream content = connection.getInputStream();
            BufferedReader in   =
                    new BufferedReader (new InputStreamReader(content));
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println(line);
            }


        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
