package kafka.util;

import java.io.*;
import java.util.Scanner;

/**
 * 本地存储读取Kafka的offset信息Utils
 */
public class txtIOUtil {

    public static void main(String args[]) {
        txtIOUtil.readLastLine("D:/tr.txt");
//        try {
//            txtIOUtil.rewriteendline("D:/tr.txt","333");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    public static String readLastLine(String path){
        Scanner sc= null;
        try {
            sc = new Scanner(new FileReader(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line=null;
        while((sc.hasNextLine()&&(line=sc.nextLine())!=null)){
            if(!sc.hasNextLine())
                System.out.println(line);
        }
        return line;
    }

    public static void writeLastLine(String path,String content){
        FileReader fr = null;
        try {
            fr = new FileReader(new File(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedReader br = new BufferedReader(fr);
        String s;
        StringBuilder sb = new StringBuilder();
        int i = 0;
        //读文件内容
        try {
        while((s=br.readLine())!=null){
            sb.append(s).append("\n");
        }
        //对内容进行截取去掉最后一个\n,然后截取去掉最后一行，最后加入你想要的内容
        System.out.println(sb.substring(0, sb.toString()
                .substring(0, sb.length()-1).lastIndexOf("\n"))+"\n"+content);

            fr.close();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 在末行添加
     * @param filepath
     * @param string
     * @throws Exception
     */
    public static void writeendline(String filepath, String string)
            throws Exception {

        RandomAccessFile file = new RandomAccessFile(filepath, "rw");
        long len = file.length();
        long start = file.getFilePointer();
        long nextend = start + len - 1;
        byte[] buf = new byte[1];
        file.seek(nextend);
        file.read(buf, 0, 1);

        if (buf[0] == '\n')

            file.writeBytes(string);
        else

            file.writeBytes("\r\n"+string);

        file.close();

    }

    /**
     * 覆盖末行数据
     * @param filepath
     * @param string
     * @throws Exception
     */
    public static void rewriteendline(String filepath, String string)
            throws Exception {

        RandomAccessFile file = new RandomAccessFile(filepath, "rw");
        long len = file.length();
        long start = file.getFilePointer();
        long nextend = start + len - 1;

        int i = -1;
        file.seek(nextend);
        byte[] buf = new byte[1];

        while (nextend > start) {

            i = file.read(buf, 0, 1);
            if (buf[0] == '\r') {
                file.setLength(nextend - start);
                break;
            }
            nextend--;
            file.seek(nextend);
        }
        file.close();
        writeendline(filepath, string);
    }
}
