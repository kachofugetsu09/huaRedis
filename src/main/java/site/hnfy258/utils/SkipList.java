package site.hnfy258.utils;

import org.apache.log4j.Logger;

import java.util.*;

public class SkipList implements Cloneable{
    private static final int MAX_LEVEL = 32;
    private static final double P = 0.25;
    private Node head;
    private int level;
    private Random random;
    private int size;

    Logger logger = Logger.getLogger(SkipList.class);

    public int size() {
        return size;
    }

    public SkipList deepCopy() {
        try {
            SkipList clone = (SkipList) super.clone();
            clone.head = new Node(Double.NEGATIVE_INFINITY, null);
            clone.random = new Random();
            clone.level = this.level;
            clone.size = this.size;

            // 先创建所有节点的副本
            Map<Node, Node> nodeMap = new HashMap<>();
            Node current = this.head.next[0];
            while (current != null) {
                Node newNode = new Node(current.score, current.member);
                nodeMap.put(current, newNode);
                current = current.next[0];
            }

            // 设置链接关系
            for (int i = 0; i < level; i++) {
                current = this.head.next[i];
                Node prev = clone.head;
                while (current != null) {
                    Node newNode = nodeMap.get(current);
                    prev.next[i] = newNode;
                    prev = newNode;
                    current = current.next[i];
                }
            }

            return clone;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }


    public static class Node {
        public double score;
        public String member;
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

    public boolean insert(double score, String member) {
        Node[] update = new Node[MAX_LEVEL];
        Node cur = head;
        for (int i = level - 1; i >= 0; i--) {
            while (cur.next[i] != null && (cur.next[i].score < score
                    || (cur.next[i].score == score && cur.next[i].member.compareTo(member) < 0))) {
                cur = cur.next[i];
            }
            update[i] = cur;
        }
        cur = cur.next[0];

        if (cur != null && cur.member.equals(member)) {
            // 如果member已存在,更新其score
            cur.score = score;
            return false; // 返回false表示这是一个更新操作,而不是新插入
        }

        int newLevel = randomLevel();
        if (newLevel > level) {
            for (int i = level; i < newLevel; i++) {
                update[i] = head;
            }
            level = newLevel;
        }
        Node newNode = new Node(score, member);
        for (int i = 0; i < newLevel; i++) {
            newNode.next[i] = update[i].next[i];
            update[i].next[i] = newNode;
        }
        size++;
        return true; // 返回true表示这是一个新插入操作
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

            while(level>1&&head.next[level-1]==null){
                level--;
            }
            size--;
            print();
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

    public boolean containsMember(String member) {
        Node current = head;
        for (int i = level - 1; i >= 0; i--) {
            while (current.next[i] != null && current.next[i].member.compareTo(member) < 0) {
                current = current.next[i];
            }
        }
        current = current.next[0];
        return current != null && current.member.equals(member);
    }

    public List<Node> getRange(int start, int stop) {
        List<Node> result = new ArrayList<>();
        Node current = head.next[0];
        int index = 0;

        // 跳过start之前的元素
        while (current != null && index < start) {
            current = current.next[0];
            index++;
        }

        // 收集范围内的元素
        while (current != null && index <= stop) {
            result.add(current);
            current = current.next[0];
            index++;
        }

        return result;
    }

    public List<Node> getRangeByScore(int min, int max) {
        List<Node> result = new ArrayList<>();
        Node current = head.next[0];
        while(current!=null&&current.score<=min){
            current=current.next[0];
        }
        while(current!=null&&current.score<=max){
            result.add(current);
            current=current.next[0];
        }
        return result;
    }

    public void print() {
        for (int i = level-1; i >= 0; i--) {
            Node node = head.next[i];
            System.out.print("Level " + i + ": ");
            while (node != null) {
                System.out.print("(" + node.member + "," + node.score + ") ");
                node = node.next[i];
            }
            System.out.println();
        }
    }

}
