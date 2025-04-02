package site.hnfy258.protocal;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

public class ProtocalRead {
    private ByteBufInputStream in;

    public ProtocalRead(ByteBufInputStream in){
        this.in = in;
    }
    //:0\r\n
    public long readInteger() throws Exception {
        int b = in.read();
        if (b == -1) {
            throw new RuntimeException("不应该读到文件末尾");
        }

        StringBuilder  sb = new StringBuilder();
        boolean NegativeFlag = false;
        if (b == '-') {
            NegativeFlag = true;
        } else {
            sb.append((char) b);
        }
        while(true){
            b = in.read();
            if(b==-1){
                throw new RuntimeException("不应该读到文件末尾");
            }
            if(b=='\r'){
                int c = in.read();
                if(c=='\n'){
                    break;
                }else{
                    throw new RuntimeException("应该读到\\r\\n");
                }
            }else{
                sb.append((char) b);
            }
        }
        long rs = Long.parseLong(sb.toString());
        if(NegativeFlag==true){
            rs = -rs;
        }
        return rs;

    }
}
