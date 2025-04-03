package site.hnfy258.command;

import java.util.function.Supplier;

public enum CommandType
 {

     ;
 
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