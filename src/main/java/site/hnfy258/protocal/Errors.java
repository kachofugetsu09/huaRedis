package site.hnfy258.protocal;


public class Errors extends Resp
{
    String content;

    public Errors(String content)
    {
        this.content = content;
    }

    public String getContent()
    {
        return content;
    }
}
