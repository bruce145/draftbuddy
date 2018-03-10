package UpdateManager;


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.io.*;
import java.sql.*;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by Bruce on 11/8/2017.
 */
public class NHLStatJsonParser {

    private static String dbConnectionString = "";

    private static Connection conn;

    public NHLStatJsonParser(String connection){
        dbConnectionString = connection;
    }


//---------------------------INSERT NEW RECORDS---------------------------------//

    private void insertGoalToDB(JsonNode g, int gameID, int homeId, int awayId){

        PreparedStatement pstmt;
        String query = "INSERT INTO EventCurrent VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        JsonNode playerList = g.findValue("players");
        int scorerTeamId = g.findValue("team").path("id").asInt();
        int oppTeamId;
        String eventType = "SHOT_ATTEMPT";
        String result = "GOAL";

        if(scorerTeamId == homeId)
            oppTeamId = awayId;
        else
            oppTeamId = homeId;

        try{
            pstmt = conn.prepareStatement(query);
            pstmt.setInt(1, gameID);
            pstmt.setInt(2, scorerTeamId);
            pstmt.setInt(3, playerList.get(0).findValue("id").asInt());
            pstmt.setString(4, eventType);
            pstmt.setString(5, g.findValue("result").path("secondaryType").asText());
            pstmt.setString(6, result);
            pstmt.setInt(7, g.findValue("coordinates").path("x").asInt());
            pstmt.setInt(8,g.findValue("coordinates").path("y").asInt());
            pstmt.setInt(9,g.findValue("period").asInt());
            pstmt.setInt(10, TOItoInt(g.findValue("periodTime").asText()));
            pstmt.setInt(14, oppTeamId);
            pstmt.setInt(15,0);//no penalty


            switch (playerList.size()){

                case 1://empty net goal
                    pstmt.setInt(11, 0);
                    pstmt.setInt(12,0);
                    pstmt.setInt(13, 0);
                    break;
                case 2://goal no assist
                   pstmt.setInt(11, 0);
                   pstmt.setInt(12,0);
                   pstmt.setInt(13, playerList.get(1).findValue("id").asInt());
                   break;
                case 3://goal 1 assist
                    pstmt.setInt(11, playerList.get(1).findValue("id").asInt());
                    pstmt.setInt(12,0);
                    pstmt.setInt(13, playerList.get(2).findValue("id").asInt());
                    break;
                case 4://goal 2 assist
                    pstmt.setInt(11, playerList.get(1).findValue("id").asInt());
                    pstmt.setInt(12,playerList.get(2).findValue("id").asInt());
                    pstmt.setInt(13, playerList.get(3).findValue("id").asInt());
                    break;
            }


            if(g.findValue("period").asInt() == 5){
                pstmt.setInt(16, 0);
                pstmt.setInt(17,0);
                pstmt.setInt(18,1);
                pstmt.setString(19, "SHOOTOUT");
            }else {

                int emptyNet = g.findValue("emptyNet").asBoolean() ? 1 : 0;
                int gwg = g.findValue("gameWinningGoal").asBoolean() ? 1 : 0;

                pstmt.setInt(16, emptyNet);
                pstmt.setInt(17, gwg);
                pstmt.setInt(18,0);
                pstmt.setString(19, g.findValue("result").findValue("strength").path("code").asText());
            }

            pstmt.executeUpdate();

        }catch (SQLException e){e.printStackTrace(); return;}

    }

    private void insertShotToDB(JsonNode g, int gameID, int homeId, int awayId){

        PreparedStatement pstmt;
        String query = "INSERT INTO EventCurrent VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        JsonNode playerList = g.findValue("players");
        int scorerTeamId = g.findValue("team").path("id").asInt();
        int oppTeamId;
        String eventType = "SHOT_ATTEMPT";
        String result = g.findValue("eventTypeId").asText();


        if(scorerTeamId == homeId)
            oppTeamId = awayId;
        else
            oppTeamId = homeId;



        try{
            pstmt = conn.prepareStatement(query);
            pstmt.setInt(1, gameID);
            pstmt.setInt(2, scorerTeamId);
            pstmt.setInt(3, playerList.get(0).findValue("id").asInt());
            pstmt.setString(4, eventType);
            pstmt.setString(5, g.findValue("result").path("secondaryType").asText());
            pstmt.setString(6, result);
            pstmt.setInt(7, g.findValue("coordinates").path("x").asInt());
            pstmt.setInt(8,g.findValue("coordinates").path("y").asInt());
            pstmt.setInt(9,g.findValue("period").asInt());
            pstmt.setInt(10, TOItoInt(g.findValue("periodTime").asText()));
            pstmt.setInt(11,0);
            pstmt.setInt(12,0);

            try {
                pstmt.setInt(13, playerList.get(1).path("player").path("id").asInt());
            }catch (NullPointerException e){ pstmt.setInt(13, 0);}
            pstmt.setInt(14,oppTeamId);
            pstmt.setInt(15,0);
            pstmt.setInt(16,0);
            pstmt.setInt(17,0);
            pstmt.setInt(18,0);
            pstmt.setInt(19,0);



            pstmt.executeUpdate();

        }catch (SQLException e){e.printStackTrace(); return;}


    }

    private  void insertHitPimToDB(JsonNode g, int gameID, int homeId, int awayId){

        PreparedStatement pstmt;
        String query = "INSERT INTO EventCurrent VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        JsonNode playerList = g.findValue("players");
        int scorerTeamId = g.findValue("team").path("id").asInt();
        int oppTeamId;
        String eventType = g.findValue("result").path("eventTypeId").asText();

        if(scorerTeamId == homeId)
            oppTeamId = awayId;
        else
            oppTeamId = homeId;



        try{

            pstmt = conn.prepareStatement(query);
            pstmt.setInt(1, gameID);
            pstmt.setInt(2, scorerTeamId);
            pstmt.setInt(3, playerList.get(0).findValue("id").asInt());
            pstmt.setString(4, eventType);
            pstmt.setInt(7, g.findValue("coordinates").path("x").asInt());
            pstmt.setInt(8,g.findValue("coordinates").path("y").asInt());
            pstmt.setInt(9,g.findValue("period").asInt());
            pstmt.setInt(10, TOItoInt(g.findValue("periodTime").asText()));
            pstmt.setInt(11,0);
            pstmt.setInt(12,0);

            try {
                pstmt.setInt(13, playerList.get(1).path("player").path("id").asInt());
            }catch (NullPointerException e){ pstmt.setInt(13, 0);}
            pstmt.setInt(14,oppTeamId);

            pstmt.setInt(16,0);
            pstmt.setInt(17,0);
            pstmt.setInt(18,0);
            pstmt.setInt(19,0);



            switch (eventType){

                case "HIT":
                    pstmt.setString(5, "");
                    pstmt.setString(6, "");
                    pstmt.setInt(13,playerList.get(1).findValue("id").asInt());
                    pstmt.setInt(15, 0);
                    break;
                case "PENALTY":
                    pstmt.setString(5, g.findValue("result").path("secondaryType").asText());
                    pstmt.setString(6,"PENALTY");
                    try {
                        pstmt.setInt(13, playerList.get(1).findValue("id").asInt());
                    }catch (NullPointerException e){pstmt.setInt(13,0);}

                    pstmt.setInt(15, g.findValue("penaltyMinutes").asInt());
                    break;

            }
            pstmt.executeUpdate();
        }catch (SQLException e){e.printStackTrace(); return;}
    }

    private  void insertGTFToDB(JsonNode g, int gameID, int homeId, int awayId){

        PreparedStatement pstmt;
        String query = "INSERT INTO EventCurrent VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        JsonNode playerList = g.findValue("players");
        int scorerTeamId = g.findValue("team").path("id").asInt();
        int oppTeamId;
        String eventType = g.findValue("result").path("eventTypeId").asText();

        if(scorerTeamId == homeId)
            oppTeamId = awayId;
        else
            oppTeamId = homeId;



        try{
            pstmt = conn.prepareStatement(query);
            pstmt.setInt(1, gameID);
            pstmt.setInt(2, scorerTeamId);
            pstmt.setInt(3, playerList.get(0).findValue("id").asInt());
            pstmt.setString(4, eventType);
            pstmt.setString(5, "");
            pstmt.setString(6,"");
            pstmt.setInt(7, g.findValue("coordinates").path("x").asInt());
            pstmt.setInt(8,g.findValue("coordinates").path("y").asInt());
            pstmt.setInt(9,g.findValue("period").asInt());
            pstmt.setInt(10, TOItoInt(g.findValue("periodTime").asText()));
            pstmt.setInt(11,0);
            pstmt.setInt(12,0);
            pstmt.setInt(15,0);
            pstmt.setInt(16,0);
            pstmt.setInt(17,0);
            pstmt.setInt(18,0);
            pstmt.setInt(19,0);




            switch (eventType){

                case "GIVEAWAY":
                case "TAKEAWAY":
                    pstmt.setInt(13,0);
                    pstmt.setInt(14,oppTeamId);
                    break;

                case "FACEOFF":
                    pstmt.setInt(13, playerList.get(1).findValue("id").asInt());
                    pstmt.setInt(14,oppTeamId);
                    break;

            }

            pstmt.executeUpdate();



        }catch (SQLException e){e.printStackTrace(); return;}

    }

    private void insertPlayerGameLog(String season, String gameId){


        String jsonPath = "src/liveGame/" + season + "/" + gameId + ".json";
        JsonFactory jFactory = new JsonFactory();
        JsonParser jParse;
        JsonNode rootNode;
        JsonNode[] players = new JsonNode[2];
        int tidArr[] = new int[2];
        int gid = 0;


        try {
            jParse = jFactory.createParser(new File(jsonPath));
            jParse.setCodec(new ObjectMapper());
            rootNode = jParse.readValueAsTree();
            gid = rootNode.findValue("pk").asInt();
            players[0] = rootNode.findValue("boxscore").findValue("home").findValue("players");
            tidArr[0] = rootNode.findValue("boxscore").findValue("home").findValue("id").asInt();
            players[1] = rootNode.findValue("boxscore").findValue("away").findValue("players");
            tidArr[1] = rootNode.findValue("boxscore").findValue("away").findValue("id").asInt();
        } catch (IOException | NullPointerException e) {
            e.printStackTrace();
        }


        Connection conn = dbConnect();
        PreparedStatement pstmt;


        String query = "INSERT INTO PlayerGameLogCurrent VALUES " +
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        for(int i = 0; i < players.length; i++) {

            int tid = tidArr[i];

            for (JsonNode p : players[i]) {

                if(p.findValue("stats").size() < 1)
                    continue;
                try {
                    pstmt = conn.prepareStatement(query);
                    pstmt.setInt(1, gid);
                    pstmt.setInt(2, tid);
                    pstmt.setInt(3,p.findValue("id").asInt());
                    pstmt.setInt(19, TOItoInt(p.findValue("timeOnIce").asText()));
                    pstmt.setInt(4, p.findValue("goals").asInt());
                    pstmt.setInt(5, p.findValue("assists").asInt());


                    if (p.findValue("type").asText().equals("Goalie")) {
                        int ga = p.findValue("shots").asInt() - p.findValue("saves").asInt();
                        //player categories only
                        pstmt.setInt(6,0);
                        pstmt.setInt(7,0);
                        pstmt.setInt(8,0);
                        pstmt.setInt(9, 0);
                        pstmt.setInt(10,0);
                        pstmt.setInt(11, 0);
                        pstmt.setInt(12,0);
                        pstmt.setInt(13,0);
                        pstmt.setInt(14, 0);
                        pstmt.setInt(15, 0);
                        pstmt.setInt(16, 0);
                        pstmt.setInt(17, 0);
                        pstmt.setInt(18, 0);
                        pstmt.setString(20,"0");
                        pstmt.setString(21, "0");
                        //goalie categories only
                        pstmt.setInt(22,ga);
                        pstmt.setInt(23,p.findValue("saves").asInt());
                        pstmt.setInt(24,p.findValue("evenSaves").asInt());
                        pstmt.setInt(25,p.findValue("evenShotsAgainst").asInt()
                                - p.findValue("evenSaves").asInt());
                        pstmt.setInt(26,p.findValue("shortHandedSaves").asInt());
                        pstmt.setInt(27, p.findValue("shortHandedShotsAgainst").asInt()
                                - p.findValue("shortHandedSaves").asInt());
                        pstmt.setInt(28, p.findValue("powerPlaySaves").asInt());
                        pstmt.setInt(29,p.findValue("powerPlayShotsAgainst").asInt()
                        - p.findValue("powerPlaySaves").asInt());
                        if(p.findValue("decision").asText().equals("W")){
                            pstmt.setInt(30,1);
                            pstmt.setInt(31,0);
                        }else{
                            pstmt.setInt(30,0);
                            pstmt.setInt(31,1);
                        }


                    }else{
                        //player categories only
                        pstmt.setInt(6, p.findValue("shots").asInt());
                        pstmt.setInt(7, p.findValue("powerPlayGoals").asInt());
                        pstmt.setInt(8, p.findValue("powerPlayAssists").asInt());
                        pstmt.setInt(9, p.findValue("plusMinus").asInt());
                        pstmt.setInt(10,p.findValue("shortHandedGoals").asInt());
                        pstmt.setInt(11, p.findValue("shortHandedAssists").asInt());
                        pstmt.setInt(12,p.findValue("faceOffWins").asInt());
                        pstmt.setInt(13,p.findValue("faceoffTaken").asInt());
                        pstmt.setInt(14, p.findValue("takeaways").asInt());
                        pstmt.setInt(15, p.findValue("giveaways").asInt());
                        pstmt.setInt(16, p.findValue("skaterStats").path("penaltyMinutes").asInt());
                        pstmt.setInt(17, p.findValue("hits").asInt());
                        pstmt.setInt(18, p.findValue("blocked").asInt());
                        pstmt.setInt(20,TOItoInt(p.findValue("shortHandedTimeOnIce").asText()));
                        pstmt.setInt(21, TOItoInt(p.findValue("powerPlayTimeOnIce").asText()));
                        //goalie categories only
                        pstmt.setInt(22,0);
                        pstmt.setInt(23,0);
                        pstmt.setInt(24,0);
                        pstmt.setInt(25,0);
                        pstmt.setInt(26,0);
                        pstmt.setInt(27, 0);
                        pstmt.setInt(28, 0);
                        pstmt.setInt(29,0);
                        pstmt.setInt(30,0);
                        pstmt.setInt(31,0);
                    }

                    pstmt.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void insertBoxScore(String season, String gameId){
        String jsonPath = "src/liveGame/" + season + "/" + gameId + ".json";
        JsonFactory jFactory = new JsonFactory();
        JsonParser jParse;
        JsonNode rootNode = null;
        JsonNode lineScore;
        JsonNode teams[] = new JsonNode[2];
        int gid = 0;
        int finalPeriod = 0;
        int winnerTid = 0;
        int hTid = 0;
        int aTid = 0;
        String venue = null;


        try {
            jParse = jFactory.createParser(new File(jsonPath));
            jParse.setCodec(new ObjectMapper());
            rootNode = jParse.readValueAsTree();
            lineScore = rootNode.findValue("linescore");
            finalPeriod = lineScore.findValue("currentPeriod").asInt();
            teams[0] = lineScore.findValue("teams").findValue("home");
            teams[1] = lineScore.findValue("teams").findValue("away");
            hTid = teams[0].findValue("id").asInt();
            aTid = teams[1].findValue("id").asInt();
            venue = rootNode.findValue("gameData").findValue("home").findValue("venue").findValue("name").asText();
            gid = rootNode.findValue("pk").asInt();

            if(teams[0].findValue("goals").asInt() > teams[1].findValue("goals").asInt())
                winnerTid = hTid;
            else
                winnerTid = aTid;

        } catch (IOException | NullPointerException e) {
            e.printStackTrace();
        }


        PreparedStatement pstmt;


        String query = "INSERT INTO GameLogCurrent VALUES " +
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try{
            pstmt = conn.prepareStatement(query);
            pstmt.setInt(1,gid);
            pstmt.setString(2, venue);
            pstmt.setInt(3,teams[0].findValue("id").asInt());
            pstmt.setInt(4, teams[0].findValue("goals").asInt());
            pstmt.setInt(5, teams[0].findValue("shotsOnGoal").asInt());
            pstmt.setInt(6, teams[0].findValue("goaliePulled").asText().equals("true") ? 1 : 0 );
            pstmt.setInt(11,teams[1].findValue("id").asInt());
            pstmt.setInt(12, teams[1].findValue("goals").asInt());
            pstmt.setInt(13, teams[1].findValue("shotsOnGoal").asInt());
            pstmt.setInt(14, teams[1].findValue("goaliePulled").asText().equals("true") ? 1 : 0 );
            pstmt.setString(19, rootNode.findValue("dateTime").asText());

            if(finalPeriod < 4){
                if(winnerTid == hTid) {
                    pstmt.setInt(7, 1);//hT reg win
                    pstmt.setInt(8, 0);
                    pstmt.setInt(9, 0);//hT otw
                    pstmt.setInt(10, 0);
                    pstmt.setInt(15,0);//aT reg win
                    pstmt.setInt(16,1);
                    pstmt.setInt(17,0);//aT otw
                    pstmt.setInt(18,0);
                }else{
                    pstmt.setInt(7, 0);//hT reg win
                    pstmt.setInt(8, 1);
                    pstmt.setInt(9, 0);//hT otw
                    pstmt.setInt(10, 0);
                    pstmt.setInt(15,1);//aT reg win
                    pstmt.setInt(16,0);
                    pstmt.setInt(17,0);//aT otw
                    pstmt.setInt(18,0);
                }
            }else{
                if(winnerTid == hTid) {
                    pstmt.setInt(7, 0);//hT reg win
                    pstmt.setInt(8, 0);
                    pstmt.setInt(9, 1);//hT otw
                    pstmt.setInt(10, 0);
                    pstmt.setInt(15,0);//aT reg win
                    pstmt.setInt(16,0);
                    pstmt.setInt(17,0);//aT otw
                    pstmt.setInt(18,1);
                }else{
                    pstmt.setInt(7, 0);//hT reg win
                    pstmt.setInt(8, 0);
                    pstmt.setInt(9, 0);//hT otw
                    pstmt.setInt(10, 1);
                    pstmt.setInt(15,0);//aT reg win
                    pstmt.setInt(16,0);
                    pstmt.setInt(17,1);//aT otw
                    pstmt.setInt(18,0);
                }
            }

            pstmt.executeUpdate();

        }catch (SQLException | NullPointerException e){e.printStackTrace();}
    }

//----------------------------UPDATE EXISTING RECORDS--------------------------//

    private void updatePlayerDB(String season, String gameId){
        String jsonPath = "src/liveGame/" + season + "/" + gameId + ".json";
        JsonFactory jFactory = new JsonFactory();
        JsonParser jParse;
        JsonNode rootNode;
        JsonNode players = null;


        try {
            jParse = jFactory.createParser(new File(jsonPath));
            jParse.setCodec(new ObjectMapper());
            rootNode = jParse.readValueAsTree();

            players = rootNode.findValue("gameData").findValue("players");
        }catch (IOException | NullPointerException e){e.printStackTrace();}

        String query = "BEGIN TRAN" +
                " IF NOT exists (SELECT * FROM PlayerCurrent WITH (UPDLOCK,SERIALIZABLE) WHERE pid = ?) " +
                "BEGIN INSERT INTO PlayerCurrent (pid, lastName, firstName, jerseyNo, " +
                "position, height, weight, birthDate, age, birthCountry, isRookie, tid, shoots, captain, aCaptain) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) END " +
                "ELSE BEGIN UPDATE PlayerCurrent SET position = ?, height = ?, weight = ?, age = ?, isRookie= ?, " +
                "tid = ?, captain = ?, aCaptain = ? WHERE pid = ? END COMMIT TRAN;";

        Connection conn = dbConnect();
        PreparedStatement pstmt;


        for (JsonNode p : players) {
            try {
                pstmt = conn.prepareStatement(query);
                pstmt.setInt(1, p.findValue("id").asInt());
                pstmt.setInt(2, p.findValue("id").asInt());
                pstmt.setString(3,p.findValue("lastName").asText());
                pstmt.setString(4, p.findValue("firstName").asText());
                pstmt.setInt(5,p.path("primaryNumber").asInt());
                pstmt.setString(6, p.findValue("primaryPosition").findValue("abbreviation").asText());
                pstmt.setString(7,p.findValue("height").asText());
                pstmt.setString(8,p.findValue("weight").asText());
                pstmt.setString(9, p.findValue("birthDate").asText());
                pstmt.setInt(10, p.path("currentAge").asInt());
                pstmt.setString(11, p.findValue("birthCountry").asText());
                pstmt.setString(12,p.findValue("rookie").asText());
                pstmt.setInt(13, p.path("currentTeam").path("id").asInt());
                pstmt.setString(14, p.path("shootsCatches").asText());
                pstmt.setString(15,p.path("captain").asText());
                pstmt.setString(16,p.path("alternateCaptain").asText());
                pstmt.setString(17, p.findValue("primaryPosition").findValue("abbreviation").asText());
                pstmt.setString(18,p.findValue("height").asText());
                pstmt.setString(19,p.findValue("weight").asText());
                pstmt.setInt(20, p.path("currentAge").asInt());
                pstmt.setString(21,p.findValue("rookie").asText());
                pstmt.setInt(22, p.path("currentTeam").path("id").asInt());
                pstmt.setString(23,p.path("captain").asText());
                pstmt.setString(24,p.path("alternateCaptain").asText());
                pstmt.setInt(25, p.findValue("id").asInt());

                pstmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void updateTeamGameLog(String season, String gameId){

        String jsonPath = "src/liveGame/" + season + "/" + gameId + ".json";
        JsonFactory jFactory = new JsonFactory();
        JsonParser jParse;
        JsonNode rootNode;
        JsonNode teams = null;


        try {
            jParse = jFactory.createParser(new File(jsonPath));
            jParse.setCodec(new ObjectMapper());
            rootNode = jParse.readValueAsTree();

            teams = rootNode.findValue("gameData").findValue("teams");
        }catch (IOException | NullPointerException e){e.printStackTrace();}

        Connection conn = dbConnect();
        int hTid = teams.findValue("home").findValue("id").asInt();
        int aTid = teams.findValue("away").findValue("id").asInt();



        PreparedStatement pstmt, pstmt2;


        String query = "SELECT tid, MAX(Win), MAX(Loss), SUM(G), SUM(Sh), SUM(PPG), SUM(SHG)," +
                "SUM(FOW), SUM(FOTaken), SUM(takeAway), SUM(giveAway), SUM(PIM), SUM(Ht),"+
                "SUM(Blk), SUM(GA), SUM(evGA), SUM(shGA), SUM(ppGA) FROM PlayerGameLogCurrent" +
                " WHERE gid = " + gameId + " GROUP BY tid;";


        String query2 = "INSERT INTO TeamGameLogCurrent VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

        ResultSet rs;
        try {
            pstmt = conn.prepareStatement(query);

            rs = pstmt.executeQuery();

            while(rs.next()){
                pstmt2 = conn.prepareStatement(query2);
                pstmt2.setInt(1, Integer.parseInt(gameId));
                for(int i = 1; i < 19;i++) {
                    pstmt2.setInt(i + 1, rs.getInt(i));

                }

                pstmt2.executeUpdate();
            }
        }catch (SQLException | NullPointerException e){e.printStackTrace();}
    }

    private void updateTeamDB(){

        String jsonPath = "src/liveGame/teamList.json";
        JsonFactory jFactory = new JsonFactory();
        JsonParser jParse;
        JsonNode rootNode;
        JsonNode teams = null;


        try {
            jParse = jFactory.createParser(new File(jsonPath));
            jParse.setCodec(new ObjectMapper());
            rootNode = jParse.readValueAsTree();
            teams = rootNode.findValue("teams");
        }catch (IOException | NullPointerException e){e.printStackTrace();}

        Connection conn = dbConnect();
        PreparedStatement pstmt;



        String query = "INSERT INTO TeamCurrent VALUES " +
                "(?,?,?,?,?,?,?)";

        try {
            pstmt = conn.prepareStatement(query);

            for (JsonNode t : teams) {
                pstmt.setInt(1, t.findValue("id").asInt());
                pstmt.setString(2, t.findValue("abbreviation").asText());
                pstmt.setString(3, t.findValue("locationName").asText());
                pstmt.setString(4,t.findValue("teamName").asText());
                pstmt.setString(5,t.findValue("venue").findValue("name").asText());
                pstmt.setString(6, t.findValue("conference").findValue("name").asText());
                pstmt.setString(7, t.findValue("division").findValue("name").asText());
                pstmt.executeUpdate();
            }

        }catch (SQLException e){e.printStackTrace();}



    }

    private void playParser(String season, String gameId){

        String jsonPath = "src/liveGame/" + season + "/" + gameId + ".json";
        JsonFactory jFactory = new JsonFactory();
        JsonParser jParse;
        JsonNode rootNode;
        JsonNode plays = null;
        int gid = Integer.parseInt(gameId);
        int homeId = 0;
        int awayId = 0;

        try {
            jParse = jFactory.createParser(new File(jsonPath));
            jParse.setCodec(new ObjectMapper());
            rootNode = jParse.readValueAsTree();
            homeId = rootNode.findValue("teams").path("home").path("id").asInt();
            awayId = rootNode.findValue("teams").path("away").path("id").asInt();

            plays = rootNode.findValue("allPlays");
        }catch (IOException | NullPointerException e){e.printStackTrace();}


        for (JsonNode e : plays) {
            int i = 0;

            switch (e.findValue("eventTypeId").asText()){

                case "GOAL":
                    insertGoalToDB(e, gid, homeId, awayId);
                    break;

                case "SHOT":
                case "MISSED_SHOT":
                case "BLOCKED_SHOT":
                    insertShotToDB(e, gid, homeId, awayId);
                    break;

                case "GIVEAWAY":
                case "TAKEAWAY":
                case "FACEOFF":
                    insertGTFToDB(e,gid,homeId,awayId);
                    break;


                case "PENALTY":
                case "HIT":
                    insertHitPimToDB(e, gid, homeId, awayId);
                    break;



            }
        }

    }

    private void bulkUpdate(int last, int latest){
            for(int i = 1; i < 6; i++) {
                operationSelect(i, 17, last + 1, latest);
            }
    }

 //---------------------------Helper Functions-----------------------------------//



    private int TOItoInt(String toi){
        int m, s;
        int totalSeconds;
        String[] time = toi.split(":");
        m = Integer.parseInt(time[0]) * 60;
        s = Integer.parseInt(time[1]);
        totalSeconds = s + m;
        return totalSeconds;
    }

//-------------------------DB and user Interface--------------------------------//

    private static String grabConnectionString(){
        Properties p = new Properties();
        FileInputStream in = null;
        try {
            in = new FileInputStream("src/config/config.txt");
        }catch (FileNotFoundException e){e.printStackTrace();}
        try {
            p.load(in);
        }catch (IOException e){e.printStackTrace();}

        return p.getProperty("CONNECTION_STRING");


    }

    private Connection dbConnect(){
        Connection conn = null;
        Properties connectionProps = new Properties();

        try {
            conn = DriverManager.getConnection(
                    dbConnectionString,
                    connectionProps);
            }catch (SQLException e){
            e.printStackTrace();
        }
        return conn;
    }

    private void operationSelect(int selection, int season, int startGame, int endGame){

        String seasonString = "20" + Integer.toString(season) + "-" + Integer.toString(season+1);

        switch (selection) {

            case 1:
                for(int i = startGame; i <= endGame; i++) {
                    String format = String.format("%04d", i);
                    String id = "20" + Integer.toString(season) + "02" + format;
                    this.playParser(seasonString, "20" + Integer.toString(season) + "02" + format);
                    System.out.println("Parsing plays for game " + id);
                }
                break;
            case 2:
                for(int i = startGame; i <= endGame; i++) {
                    String format = String.format("%04d", i);
                    String id = "20" + Integer.toString(season) + "02" + format;
                    System.out.println("Updating rosters for game " + id);
                    this.updatePlayerDB(seasonString, "20" + Integer.toString(season) + "02" + format);
                }
                break;
            case 3:
                for(int i = startGame; i <= endGame; i++) {
                    String format = String.format("%04d", i);
                    String id = "20" + Integer.toString(season) + "02" + format;
                    System.out.println("Inserting player game logs for game " + id);
                    this.insertPlayerGameLog(seasonString, "20" + Integer.toString(season) + "02" + format);
                }
                break;
            case 4:
                for(int i = startGame; i <= endGame; i++) {
                    String format = String.format("%04d", i);
                    String id = "20" + Integer.toString(season) + "02" + format;
                    System.out.println("Inserting boxscore for game " + id);
                    insertBoxScore(seasonString, "20" + Integer.toString(season) + "02" + format);
                }
                break;
            case 5:
                for(int i = startGame; i <= endGame; i++) {
                    String format = String.format("%04d", i);
                    String id = "20" + Integer.toString(season) + "02" + format;
                    System.out.println("Inserting team game log for game " + id);
                    updateTeamGameLog(seasonString, "20" + Integer.toString(season) + "02" + format);
                }
                break;
        }
    }

    private void manualUpdate() {
        Scanner reader = new Scanner(System.in);  // Reading from System.in
        System.out.println("Enter a number: ");
        int game = reader.nextInt();
        reader.close();
        conn = dbConnect();
        //bulkUpdate(game);
    }

    private void autoUpdate(){

        NHLStatDownloaderJsonImp downloader = new NHLStatDownloaderJsonImp();
        int lastGame = downloader.getLastGame();
        int latestGame = downloader.getLatest();
        String response = (latestGame == lastGame) ? "All games up to date" : "Latest available game is " + latestGame;
        System.out.println(response);
        conn = dbConnect();
        bulkUpdate(lastGame, latestGame);
    }

    public static void main(String args[]){
        NHLStatJsonParser n = new NHLStatJsonParser(grabConnectionString());
        //n.autoUpdate();

    }
}
