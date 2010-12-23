package okuyama.imdst.client;

/**
 * ImdstClientException<br>
 * ※注意!!<br>
 * 過去バージョンとの互換性維持の為に残していますが、次期バージョンで削除します<br>
 * okuyama.imdst.client.OkuyamaClientExceptionを利用してください.<br>
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
