package site.hnfy258.command.impl.Set;

import org.apache.log4j.Logger;
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
    
    Logger logger =  Logger.getLogger(Sadd.class);

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
            //logger.info("Handling SADD command for key: " + key);
            RedisData redisData = redisCore.get(key);
            if(redisData == null){
                //logger.info("Creating new RedisSet for key: " + key);
                RedisSet redisSet = new RedisSet();
                int sadd = redisSet.sadd(members);
                redisCore.put(key, redisSet);
                //logger.info("Added " + sadd + " members to new set");
                return new RespInt(sadd);
            }
            else if(redisData instanceof RedisSet){
                //logger.info("Adding to existing RedisSet for key: " + key);
                RedisSet redisSet = (RedisSet) redisData;
                int sadd = redisSet.sadd(members);
                //logger.info("Added " + sadd + " members to existing set");
                return new RespInt(sadd);
            }
            else{
                //logger.info("Wrong type for key: " + key);
                return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        } catch (Exception e) {
            logger.error("Error handling SADD command: " + e.getMessage());
            return new Errors("ERR " + e.getMessage());
        }
    }
}
