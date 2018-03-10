package UpdateManager;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.URL;

public class NHLStatDownloaderJsonImp implements NHLStatDownloader {
    private final int lastGame, latest;


    NHLStatDownloaderJsonImp(){
        this.lastGame = getLastDownloadedGameNo();
        this.latest = findLatestGameNo(lastGame);
    }

    @Override
    public int getLastGame(){ return this.lastGame; }
    @Override
    public int getLatest() {return this.latest;}
    @Override
    public String readPageToString(String urlString) throws Exception {
        BufferedReader reader = null;
        try {
            URL url = new URL(urlString);
            reader = new BufferedReader(new InputStreamReader(url.openStream()));
            StringBuilder pageString = new StringBuilder();
            int read;
            char[] chars = new char[1024];
            while ((read = reader.read(chars)) != -1)
                pageString.append(chars, 0, read);

            return pageString.toString();
        } finally {
            if (reader != null)
                reader.close();
        }
    }
    @Override
    public void gameGrabber(String gameID){
        String jsonString = null;

        try {
            jsonString = readPageToString(
                    "http://statsapi.web.nhl.com/api/v1/game/" + gameID + "/feed/live");
        }catch (Exception e){e.printStackTrace();}

        saveStringToFile(jsonString, gameID, "json");


    }
    @Override
    public void saveStringToFile(String s, String fname, String type){
        String saveDirectory = "src/liveGame/2017-18/";

        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(saveDirectory + fname + "." + type));
            out.write(s);
            out.close();
        }catch (IOException e){e.printStackTrace();}

    }
    @Override
    public int getLastDownloadedGameNo() {
        File folder = new File("src/liveGame/2017-18");
        File[] listOfFiles = folder.listFiles();
        int lastGame = 0;
        try {
            if ((listOfFiles != null ? listOfFiles.length : 0) != 0) {
                String lastDownloaded = listOfFiles[listOfFiles.length - 1].getName().split("\\.")[0];
                lastGame = Integer.parseInt(lastDownloaded.substring(6));
            }
        }catch (NullPointerException e){System.out.println("Invalid directory, unable to find last downloaded game file");}
        return lastGame;
    }
    @Override
    public int findLatestGameNo(int lastDownloaded) {
        System.out.println("Finding latest available game");
        String jsonString;
        JsonFactory jFactory = new JsonFactory();
        JsonParser jParse;
        JsonNode rootNode;
        try {
            for(int i = lastDownloaded; i < 1230; i++) {
                String format = String.format("%04d", i);
                String id = "2017" + "02" + format;
                jsonString = readPageToString(
                        "http://statsapi.web.nhl.com/api/v1/game/" + id + "/feed/live");
                jParse = jFactory.createParser(jsonString);
                jParse.setCodec(new ObjectMapper());
                rootNode = jParse.readValueAsTree();
                if(!rootNode.findValue("detailedState").asText().equals("Final"))
                    return i - 1;
            }
        }catch (Exception e){e.printStackTrace();}
        return 0;
    }
    @Override
    public void download(){

        for(int i = lastGame + 1; i <= latest; i++) {
            String format = String.format("%04d", i);
            String id = "20" + Integer.toString(17) + "02" + format;
            this.gameGrabber(id);
            System.out.println("Downloaded game " + id);
        }
    }
}
