package site.hnfy258.command.impl;

import io.netty.channel.ChannelHandlerContext;
import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Info implements Command
 {

     private RedisCore redisCore;

     public Info(RedisCore redisCore)
     {
         this.redisCore = redisCore;
     }
     @Override
     public CommandType getType()
     {
         return CommandType.INFO;
     }
 
     @Override
     public void setContext(Resp[] array)
     {

     }
 
     @Override
     public Resp handle()
     {
         List<String> list = new ArrayList<>();
         list.add("redis_version:jfire_redis_mock");
         list.add("os:" + System.getProperty("os.name"));
         list.add("process_id:" + getPid());
         Optional<String> reduce = list.stream().map(name -> name + "\r\n").reduce((first, second) -> first + second);
         String           s      = reduce.get();
         return new BulkString(new BytesWrapper(s.getBytes(StandardCharsets.UTF_8)));
     }
 
     private String getPid()
     {
         String name = ManagementFactory.getRuntimeMXBean().getName();
         String pid  = name.split("@")[0];
         return pid;
     }
 }