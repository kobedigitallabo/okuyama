package okuyama.imdst.client;

/**
 * OkuyamaClientException<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class OkuyamaClientException extends Exception {

    public OkuyamaClientException(Throwable t) {
        super(t);
    }

    public OkuyamaClientException(String msg) {
        super(msg);
    }

    public OkuyamaClientException(String msg, Throwable t) {
        super(msg, t);
    }
}
