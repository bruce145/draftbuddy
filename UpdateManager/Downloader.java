package UpdateManager;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.URL;

public class Downloader {
    private int lastGame, latest;
    private String fileType, season;
    private static HtmlScraper converter;
    private static final int SEASON_GAMES = 1230;


     Downloader(String regSeason){
         this.season = regSeason;
    }

    void downloadJSON(){
        fileType = "json";
        this.lastGame = getLastDownloadedGameNo();
        this.latest = findLatestGameNo(lastGame);
        download();
    }

    void downloadHTML(){
        fileType = "csv";
        this.lastGame = getLastDownloadedGameNo();
        this.latest = findLatestGameNo(lastGame);
        download();
    }

    private void download(){

        for(int i = lastGame + 1; i <= latest; i++) {
            String format = String.format("%04d", i);
            String id = season + "02" + format;
            if(fileType.equals("json"))
                this.gameGrabberJSON(id);
            else if(fileType.equals("csv")){
                converter = new HtmlScraper();
                this.gameGrabberHTML(id);
            }

            System.out.println("Downloaded game " + id +"." + fileType);
        }
    }

    int getLastGame(){ return this.lastGame; }

    int getLatest() {return this.latest;}

    private void gameGrabberJSON(String rawGameID){
        String jsonString = null;

        try {
            jsonString = readPageToString(
                    "http://statsapi.web.nhl.com/api/v1/game/" + rawGameID + "/feed/live");
        }catch (Exception e){e.printStackTrace();}

        saveStringToFile(jsonString, rawGameID, "json");
    }

    private void gameGrabberHTML(String rawGameID){
        String htmlString = null;
        String gameID = "PL02" + rawGameID.substring(rawGameID.length() - 4);
        String seasonId = season + Integer.toString(Integer.parseInt(season) + 1);

        try {
            htmlString = readPageToString(
                    "http://www.nhl.com/scores/htmlreports/" + seasonId + "/" + gameID + ".HTM");
        }catch (Exception e){e.printStackTrace();}

        if(htmlString != null) {
            String csvString = converter.HTMLtoCSV(htmlString);
            saveStringToFile(csvString, rawGameID, "csv");
        }


    }

    private int getLastDownloadedGameNo() {
        File folder = new File("src/liveGame/" + season);
        File[] listOfFiles = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(fileType));
        int lastGame = 0;
        try {
            if ((listOfFiles != null ? listOfFiles.length : 0) != 0) {
                String lastDownloaded = listOfFiles[listOfFiles.length - 1].getName().split("\\.")[0];
                lastGame = Integer.parseInt(lastDownloaded.substring(lastDownloaded.length() - 4));
            }
        }catch (NullPointerException e){
            System.out.println("Invalid directory, unable to find last downloaded game file");}
        return lastGame;
    }

    private int findLatestGameNo(int lastDownloaded) {
        System.out.println("Searching for latest ." + fileType + " stat file");
        String jsonString;
        JsonFactory jFactory = new JsonFactory();
        JsonParser jParse;
        JsonNode rootNode;
        int start = lastDownloaded;
        int end = SEASON_GAMES;
        //binary search for latest available game
        try {
            while(start <= end) {
                int mid = (start + end) / 2;
                String format = String.format("%04d", mid);
                String id = season + "02" + format;
                jsonString = readPageToString(
                        "http://statsapi.web.nhl.com/api/v1/game/" + id + "/feed/live");
                jParse = jFactory.createParser(jsonString);
                jParse.setCodec(new ObjectMapper());
                rootNode = jParse.readValueAsTree();
                if(!rootNode.findValue("detailedState").asText().equals("Final"))
                    end = mid - 1;
                else
                    start = mid + 1;
           }
        }catch (Exception e){e.printStackTrace();}
        return end;
    }

    private String readPageToString(String urlString) throws Exception {
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

    private void saveStringToFile(String s, String fname, String type){
        String saveDirectory = "src/liveGame/" + season + "/";

        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(saveDirectory + fname + "." + type));
            out.write(s);
            out.close();
        }catch (IOException e){e.printStackTrace();}

    }

    public static void main (String args[]){
        Downloader n = new Downloader("2013");
        n.downloadHTML();
    }
}
