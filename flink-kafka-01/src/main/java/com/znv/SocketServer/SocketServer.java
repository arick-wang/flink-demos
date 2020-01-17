package com.znv.SocketServer;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @Author: wr
 * @Date: 2020/1/16 16:38
 * @Description:
 */
public class SocketServer {
    public static void main(String[] args) {
        try {
            ServerSocket server = new ServerSocket(18080);
            Socket accept = server.accept();
            System.out.println("linked from " + accept.getInetAddress().getHostAddress());
//            InputStream inputStream = accept.getInputStream();
//            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
//            System.out.println(br.readLine());
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(accept.getOutputStream()));
            while(true) {
                bw.write("hello this is a word \n");
                bw.write(((int) (Math.random() * 100)) % 10 + "\t");
                bw.write(((int) (Math.random() * 100)) % 10 + "\n");
                bw.flush();
                Thread.sleep(1000);
            }


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
