package site.hnfy258.protocal;


import site.hnfy258.datatype.BytesWrapper;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class BulkString extends Resp {
    public static final BulkString NullBulkString = new BulkString(null);
    static final Charset CHARSET = StandardCharsets.UTF_8;
    BytesWrapper content;

    public BulkString(BytesWrapper content) {
        this.content = content;
    }

    public BytesWrapper getContent() {
        return content;
    }
}
