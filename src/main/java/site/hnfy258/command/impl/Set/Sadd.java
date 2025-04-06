package site.hnfy258.command.impl.Set;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.datatype.RedisSet;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInt;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Sadd implements Command {
    private BytesWrapper key;
    private List<BytesWrapper> members;
    RedisCore redisCore;

    public Sadd(RedisCore core){
        this.redisCore = core;
    }
    @Override
    public CommandType getType() {
        return CommandType.SADD;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
        members = Stream.of(array).skip(2).map(resp->
                ((BulkString)resp).getContent()).collect(Collectors.toList());
    }

    @Override
    public Resp handle() {
        try {
            System.out.println("Handling SADD command for key: " + key);
            RedisData redisData = redisCore.get(key);
            if(redisData == null){
                System.out.println("Creating new RedisSet for key: " + key);
                RedisSet redisSet = new RedisSet();
                int sadd = redisSet.sadd(members);
                redisCore.put(key, redisSet);
                System.out.println("Added " + sadd + " members to new set");
                return new RespInt(sadd);
            }
            else if(redisData instanceof RedisSet){
                System.out.println("Adding to existing RedisSet for key: " + key);
                RedisSet redisSet = (RedisSet) redisData;
                int sadd = redisSet.sadd(members);
                System.out.println("Added " + sadd + " members to existing set");
                return new RespInt(sadd);
            }
            else{
                System.out.println("Wrong type for key: " + key);
                return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        } catch (Exception e) {
            System.err.println("Error handling SADD command: " + e.getMessage());
            e.printStackTrace();
            return new Errors("ERR " + e.getMessage());
        }
    }
}
