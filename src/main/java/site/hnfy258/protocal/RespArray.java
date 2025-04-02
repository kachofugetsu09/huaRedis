package site.hnfy258.protocal;

public class RespArray extends Resp
{

    Resp[] array;

    public RespArray(Resp[] array)
    {
        this.array = array;
    }

    public Resp[] getArray()
    {
        return array;
    }
}
