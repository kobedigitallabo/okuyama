package okuyama.imdst.client;

/**
 * ImdstClientException<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ImdstClientException extends Exception {

    public ImdstClientException(Throwable t) {
        super(t);
    }

    public ImdstClientException(String msg) {
        super(msg);
    }

    public ImdstClientException(String msg, Throwable t) {
        super(msg, t);
    }
}
