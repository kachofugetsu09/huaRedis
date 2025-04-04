package site.hnfy258.utiils;

import java.util.Random;

public class SkipList {
    private static final int MAX_LEVEL = 32;
    private static final double P = 0.25;
    private Node head;
    private int level;
    private Random random;

    private static class Node{
        double score;
        String member;
        Node[] next;
        public Node(double score, String member) {
            this.score = score;
            this.member = member;
            this.next = new Node[MAX_LEVEL];
        }
    }

    public SkipList() {
        this.head = new Node(Double.NEGATIVE_INFINITY,null);
        this.level = 1;
        this.random = new Random();
    }

    public boolean insert(double score,String member){
        Node[] update = new Node[MAX_LEVEL];
        Node cur = head;
        //查找插入位置
        for(int i=level-1;i>=0;i--){
            //找到当前层中比score小的节点
            while (cur.next[i] != null && cur.next[i].score < score
                    || cur.next[i].score == score && cur.next[i].member.compareTo(member) < 0) {
                cur = cur.next[i];
            }
            //将当前节点赋值给update
            update[i] = cur;
        }
        cur = cur.next[0];

        //判断当前节点是否为空
        if(cur != null && cur.score == score && cur.member.equals(member)){
            return false;
        }

        int newLevel = randomLevel();
        if(newLevel > level){
            for(int i=level;i<newLevel;i++){
                update[i] = head;
            }
            level = newLevel;
        }
        Node newNode = new Node(score,member);
        for(int i=0;i<newLevel;i++){
            newNode.next[i] = update[i].next[i];
            update[i].next[i] = newNode;
        }
        return true;
    }



    public int randomLevel(){
        int lvl = 1;
        while(lvl<MAX_LEVEL && random.nextDouble() < P){
            lvl++;
        }
        return lvl;
    }


    public boolean delete(String member){
        Node[] update = new Node[MAX_LEVEL];
        Node cur = head;
        for (int i = level - 1; i >= 0; i--) {
            while (cur.next[i] != null && cur.next[i].member.compareTo(member) < 0) {
                cur = cur.next[i];
            }
            update[i] = cur;
        }

        cur = cur.next[0];

        if(cur!=null&&cur.member.equals(member)){
            for(int i=0;i<level;i++){
                if(update[i].next[i]!=cur){
                    break;
                }
                update[i].next[i] = cur.next[i];
            }

            while(level>1&&head.next[level]==null){
                level--;
            }
            return true;
        }
        return false;
    }

    public Double getScore(String member) {
        Node current = head;
        for (int i = level - 1; i >= 0; i--) {
            while (current.next[i] != null && current.next[i].member.compareTo(member) < 0) {
                current = current.next[i];
            }
        }
        current = current.next[0];
        if (current != null && current.member.equals(member)) {
            return current.score;
        }
        return null;
    }

}
