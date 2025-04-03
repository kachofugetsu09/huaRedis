package site.hnfy258.command;

import java.util.function.Supplier;
import site.hnfy258.command.impl.Ping;

public enum CommandType
 {  PING(Ping::new);

 
     private final Supplier<Command> supplier;
 
     CommandType(Supplier supplier)
     {
         this.supplier = supplier;
     }
 
     public Supplier<Command> getSupplier()
     {
         return supplier;
     }
 }