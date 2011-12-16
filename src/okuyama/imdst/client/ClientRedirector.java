package okuyama.imdst.client;


/**
 * OkuyamaClientをConnectionPoolにて利用するためのラッパークラス.<br>
 * ユーザが利用時に意識する必要はない.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ClientRedirector extends OkuyamaClient {

    public long useCount = 0L;

    public long createTime = 0L;
    public long lastUseTime = 0L;

    public boolean returnFlg = false;

    private OkuyamaClientFactory factory = null;


    public ClientRedirector(OkuyamaClientFactory factory) {
        this.factory = factory;
    }

    public void clientClose() throws OkuyamaClientException {

        super.close();
    }


    public void close() throws OkuyamaClientException {
        if (!this.returnFlg) {
            this.returnFlg = true;
            this.factory.returnConnect(this);
        }
    }

    public void incrUseCount() {
        this.useCount++;
    }
}