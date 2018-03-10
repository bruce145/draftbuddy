package UpdateManager;

/**
 * Created by Bruce on 3/6/2018.
 */
public class NHLStatDownloaderHtmlImp implements NHLStatDownloader {


    @Override
    public int getLastGame() {
        return 0;
    }

    @Override
    public int getLatest() {
        return 0;
    }

    @Override
    public String readPageToString(String url) throws Exception {
        return null;
    }

    @Override
    public void gameGrabber(String id) {

    }

    @Override
    public void saveStringToFile(String s, String fname, String type) {

    }

    @Override
    public int getLastDownloadedGameNo() {
        return 0;
    }

    @Override
    public int findLatestGameNo(int last) {
        return 0;
    }

    @Override
    public void download() {

    }
}
