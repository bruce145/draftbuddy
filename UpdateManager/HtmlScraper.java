package UpdateManager;


import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

class HtmlScraper {

      HtmlScraper() {
     }

    /**
     * Scrapes the HTML stat report for events, and returns relevant information as CSV string
     *
     * @param rawHtml HTML passed from NHLStatDownloader
     * @return CSV format string (single line)
     */
    String HTMLtoCSV(String rawHtml) {
        String csvHeader = "idx,period,elapsed time(s),event,aPid1,aPid2,aPid3,aPid4,aPid5,aPid6," +
                "hPid1,hPid2,hPid3,hPid4,hPid5,hPid6";
        StringBuilder csvString = new StringBuilder();
        csvString.append(csvHeader);

        Document doc = Jsoup.parse(rawHtml);

        try {
            Elements events = doc.getElementsByClass("evenColor");
            for(Element e : events) {
                csvString.append("\n").append(generateCSVLine(e));
            }
        }catch (NullPointerException e){e.printStackTrace();}

        return csvString.toString();

    }

    /**
     * Takes a single event element and generates a single csv line
     * @param e Passed element from HTMLToCSV
     * @return CSV line as string
     */
    private String generateCSVLine(Element e){
       String period, time, idx, eventType;
       String tempName = null;
       String csvLine;
       Elements awayPlayers, homePlayers;
       String[] awayPlayerNames = new String[6];
       String[] homePlayerNames = new String[6];


       idx = e.child(0).text();
       eventType = e.child(4).text();
       period = e.child(1).text();
       time = e.child(3).text();


       //current format of player name in html code:
       // <font style="cursor:hand;" title="Center - JONATHAN MARCHESSAULT">81</font>
       awayPlayers = e.child(6).getElementsByTag("font");//get away player elements
       homePlayers = e.child(7).getElementsByTag("font");//get home player elements

       for(int i = 0; i < 6; i++){
           //if no player name (pp, shootout, etc), then return empty string
           try {
               tempName = awayPlayers.get(i).attr("title");//find element containing player name
           }catch (IndexOutOfBoundsException ex){tempName= "null";}

           if(!tempName.equals("null")) {
             awayPlayerNames[i] = tempName;
           }
            try {
                tempName = homePlayers.get(i).attr("title");
            }catch (IndexOutOfBoundsException ex){tempName = "null";}
             homePlayerNames[i] = tempName;
       }

       csvLine = idx + "," + period + "," + time + "," + eventType + ","
               + awayPlayerNames[0] + "," + awayPlayerNames[1] + "," + awayPlayerNames[2] + "," + awayPlayerNames[3] + ","
               + awayPlayerNames[4] + "," + awayPlayerNames[5] + "," + homePlayerNames[0] + "," + homePlayerNames[1] + "," + homePlayerNames[2] + ","
               + homePlayerNames[3] + "," + homePlayerNames[4] + "," + homePlayerNames[5];

       return csvLine;

    }
}
