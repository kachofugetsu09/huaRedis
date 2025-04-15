package site.hnfy258.utils;

import site.hnfy258.datatype.BytesWrapper;

public class SDS {
    private byte[] bytes;
    private int len; //已使用
    private int alloc; //总分配

    private static final int SDS_MAX_PREALLOC = 1024*1024;

    public SDS(byte[] bytes) {
       this.len = bytes.length;
       this.alloc = calculateAlloc(this.len);
       this.bytes = new byte[this.alloc];
       System.arraycopy(bytes, 0, this.bytes, 0, this.len);
    }

    private int calculateAlloc(int len) {
        if(len <SDS_MAX_PREALLOC){
            // 至少8
            return Math.max(len * 2, 8);
        }return len+SDS_MAX_PREALLOC;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public String toString(){
        return new String(bytes,0,len, BytesWrapper.CHARSET);
    }
    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public int length(){
        return len;
    }

    public void clear(){
        this.len = 0;
    }

    public SDS append(byte[] extra){
        int newLen = this.len + extra.length;
        if(newLen > this.alloc){
            int newAlloc = calculateAlloc(newLen);
            byte[] newBytes = new byte[newAlloc];
            System.arraycopy(bytes, 0, newBytes, 0, len);
            this.bytes = newBytes;
            this.alloc = newAlloc;
        }
        System.arraycopy(extra, 0, bytes, len, extra.length);
        this.len = newLen;
        return this;
    }

    public SDS append(String extra){
        return append(extra.getBytes());
    }

    public void grow(int addLen){
        if(addLen < 0) return;
        int newLen = addLen + this.len;
        if(newLen > this.alloc){
            int newAlloc = calculateAlloc(newLen);
            byte[] newBytes = new byte[newLen];
            System.arraycopy(bytes, 0, newBytes, 0, len);
            this.bytes = newBytes;
            this.alloc = newAlloc;
        }
    }

    public SDS deepCopy(){
        SDS copy = new SDS(new byte[0]);
        copy.bytes = new byte[this.alloc];
        System.arraycopy(this.bytes, 0, copy.bytes, 0, this.len);
        copy.len = this.len;
        copy.alloc = this.alloc;
        return copy;
    }
}
