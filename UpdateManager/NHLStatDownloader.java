package UpdateManager;

/**
 * Created by Bruce on 3/8/2018.
 */
interface NHLStatDownloader {
    int getLastGame();
    int getLatest();
    String readPageToString(String url) throws Exception;
    void gameGrabber(String id);
    void saveStringToFile(String s, String fname, String type);
    int getLastDownloadedGameNo();
    int findLatestGameNo(int last);
    void download();


}
