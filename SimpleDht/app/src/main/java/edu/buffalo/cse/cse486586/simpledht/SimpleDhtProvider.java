package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.*;
import java.lang.*;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.content.Context;
import android.database.MatrixCursor;
import android.net.Uri;
import android.util.Log;


import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import java.util.Collections;
import java.util.concurrent.ExecutionException;


public class SimpleDhtProvider extends ContentProvider {


    static final String TAG = SimpleDhtProvider.class.getSimpleName();

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;

    String[] portsArray = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};

    HashMap<String, String> hmap = new HashMap<String, String>();
    ArrayList<String> hashList=new ArrayList<String>();
    //    private final Uri myUri  = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    int myPortAdress = 0;
    String avd_id = null;

    //   MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }




    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        try {
            if (hashList.size() != 1) {
                String filename = selection;
                String current_hash = genHash(filename);
                String node_Hash = null;

                node_Hash = findPosition(current_hash);
                String avd = hmap.get(node_Hash);
                int current = myPortAdress/2;
                if(avd.equals(avd_id)){
                    getContext().deleteFile(filename);
                }
                else
                {
                    String insert_msg="Delete";
                    String avd_fname = avd+"##"+filename;
                    createClientTask(insert_msg,avd_fname);
                }


            }

            else {
                String filename = selection;
                getContext().deleteFile(filename);

            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return 0;
    }
    public String findPosition (String current_hash) {
        String node_Hash = null;
        for (int i = 0; i < hashList.size(); i++) {
            String locHash = hashList.get(i);


            if (locHash.compareTo(current_hash) >= 0) {
                node_Hash = locHash;
                break;

            } else {
                if (i == (hashList.size() - 1)) {
                    node_Hash = hashList.get(0);
                }
            }

        }
        return node_Hash;
    }
    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try {
            String key = (String) values.get("key");
            String value = (String) values.get("value");
            String hashOfkey = genHash(key);
            String temp = null;

            if(hashList.size() != 1){


                Collections.sort(hashList);
                System.out.println("Hashlist size is not 1. size is## " + hashList.size());

                temp = findPosition(hashOfkey);
                String avd = hmap.get(temp);
                System.out.println("AVD## " + avd);
                int current = myPortAdress/2;
                if(avd.equals(avd_id)){
                    osw(key, value);

                }
                else {
                    String avd_fname_val = avd + "##" + key + "##" + value;
                   createClientTask("INSERT",avd_fname_val);
                }
            }

            else {

                System.out.println("Hashlist size is 1.");

                osw(key,value);

            }

        }
        catch(Exception e){
            e.printStackTrace();
        }

        return uri;
    }

   public String getmyPort(){
       TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
       String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
       String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
       return myPort;
   }

    public String getmyavd(){
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        return portStr;
    }


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try {

          String myPort = getmyPort();
          String portStr=getmyavd();
            myPortAdress = Integer.parseInt(myPort);
            avd_id = myPort;
            System.out.println("Port is## "+ myPort);
            //Node setup
            hashList.add(genHash(portStr));
           if(!myPort.equals("11108")) {
                String msg = "JoinReq";

              createClientTask(msg,myPort);

            }
            else
            {
                hmap.put(genHash(portStr),portStr);
                Log.e(TAG, "5554 is now added to Nodelist ");
            }

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);



        } catch(NoSuchAlgorithmException e){
            e.printStackTrace();
        }catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }
        return false;
    }



    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        String[] cols = {"key", "value"};
        MatrixCursor cursor = new MatrixCursor(cols);   //https##//developer.android.com/reference/android/database/MatrixCursor

        try {
            if (hashList.size() > 0) {
                BufferedReader in = null;
                if (hashList.size() == 1) {
                    if (selection.equals("@")) {

                        String path1 = getContext().getFilesDir().getAbsolutePath();
                        File fileName1 = new File(path1);

                        ArrayList<File> files = new ArrayList<File>(Arrays.asList(fileName1.listFiles()));

                        for (File file : files) {
                            if (file.isFile()) {
                                String filename = file.getName();

                                FileInputStream fis = getContext().openFileInput(filename);
                                InputStreamReader isr = new InputStreamReader(fis);
                                BufferedReader bufferedReader = new BufferedReader(isr);

                                String value = bufferedReader.readLine();
                                System.out.println("Value is## " + value);
                                Object[] eachRow = {filename, value};         //Adding key, value to array of eachRow
                                cursor.addRow(eachRow);
                                //return cursor;

                            }
                        }


                    } else if (selection.equals("*")) {
                        String path1 = getContext().getFilesDir().getAbsolutePath();
                        System.out.println(path1);
                        File fileName1 = new File(path1);

                        ArrayList<File> files = new ArrayList<File>(Arrays.asList(fileName1.listFiles()));

                        for (File file : files) {
                            if (file.isFile()) {

                                String filename = file.getName();

                                FileInputStream fis = getContext().openFileInput(filename);
                                InputStreamReader isr = new InputStreamReader(fis);
                                BufferedReader bufferedReader = new BufferedReader(isr);

                                String value = bufferedReader.readLine();
                                Object[] eachRow = {filename, value};         //Adding key, value to array of eachRow
                                cursor.addRow(eachRow);
                                //return cursor;

                            }
                        }
                    } else {
                        System.out.println("Normal query opertaion.");


                        FileInputStream fis = getContext().openFileInput(selection);
                        InputStreamReader isr = new InputStreamReader(fis);
                        BufferedReader bufferedReader = new BufferedReader(isr);
                        //StringBuilder sb = new StringBuilder();
                        String line, p = null;
                        while ((line = bufferedReader.readLine()) != null) {
                            p = line;
                        }
                        Object[] eachRow = {selection, p};         //Adding key, value to array of eachRow

                        cursor.addRow(eachRow);

                    }

                } else {
                    System.out.println("Hash List size is## " + hashList.size());
                    Collections.sort(hashList);


                    if (selection.equals("@")) {
                        String path3 = getContext().getFilesDir().getAbsolutePath();
                        System.out.println(path3);
                        File fileName3 = new File(path3);


                        ArrayList<File> files = new ArrayList<File>(Arrays.asList(fileName3.listFiles()));

                        for (File file : files) {
                            if (file.isFile()) {

                                String filename = file.getName();

                                String p = callFis(filename);
                                Object[] eachRow = {filename, p};         //Adding key, value to array of eachRow

                                cursor.addRow(eachRow);

                            }
                        }


                    } else if (selection.equals("*")) {
                        String path4 = getContext().getFilesDir().getAbsolutePath();
                        File fileName4 = new File(path4);


                        ArrayList<File> files = new ArrayList<File>(Arrays.asList(fileName4.listFiles()));

                        for (File file : files) {
                            if (file.isFile()) {

                                String filename = file.getName();

                                String value = callFis(filename);
                                Object[] eachRow = {filename, value};         //Adding key, value to array of eachRow

                                cursor.addRow(eachRow);

                            }
                        }
                        String val = String.valueOf(myPortAdress / 2);
                        String strreturned = createClientT("PerformAll", val);
                        String[] strarray = strreturned.split("-");

                        for (int i = 0; i < strarray.length; i++) {
                            String[] str = strarray[i].split("##");
                            String fname = str[0].trim();
                            String pval = str[1].trim();
                            System.out.println("Fname##Before adding## " + fname);
                            System.out.println("Pval##Before adding## " + pval);
                            Object[] eachRow = {fname, pval};         //Adding key, value to array of eachRow

                            cursor.addRow(eachRow);
                        }


                    } else {
                        System.out.println("Normal query opertaion.");


                        String filename = selection;
                        System.out.println("Selection is " + selection);
                        String node_Hash = null;
                        String current_hash = genHash(filename);
                        System.out.println("QueryingFile " + selection + " HashValue " + current_hash);
                        Collections.sort(hashList);
                        node_Hash = findPosition(current_hash);
                        String avd = hmap.get(node_Hash);


                        int current = myPortAdress / 2;

                        if (!avd.equals(avd_id)) {
                            String message2send = "Chknext";
                            String msg2send = "PresentInAvd##" + avd + "##Key##" + filename;
                            String str = createClientT(message2send, msg2send);
                            String[] strarray = str.split("##");
                            String fname = strarray[0].trim();
                            String pval = strarray[1].trim();
                            Object[] eachRow = {filename, pval};         //Adding key, value to array of eachRow

                            cursor.addRow(eachRow);
                        } else {

                            String line, p = null;
                            while ((line = callFis(selection)) != null) {
                                p = line;
                            }
                            Object[] eachRow = {filename, p};         //Adding key, value to array of eachRow

                            cursor.addRow(eachRow);
                        }
                    }


                }

            }
            } catch(FileNotFoundException e1){
                e1.printStackTrace();
            } catch(IOException e1){
                e1.printStackTrace();
            }  catch(NoSuchAlgorithmException e){
                e.printStackTrace();
            }

        return cursor;
    }
        public void createClientTask(String msg0, String msg1){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg0,msg1);
        }

        public String createClientT(String msg0, String msg1){
            String ret = null;
            try {
                ret =  new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg0,msg1).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            return ret;
        }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override


        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];



            while (true) {


                try {

                 /*
                Create a new socket to accept the ServerSocket's connection request.
                */
                    ObjectOutputStream ooStream = null;


                    Socket socket = serverSocket.accept();
                    //ObjectInputStream oiStream = new ObjectInputStream(socket.getInputStream());

                    //message = String.valueOf(oiStream.readObject());
                    InputStreamReader input = new InputStreamReader(socket.getInputStream());
                    BufferedReader reader = new BufferedReader(input);
                    String message = reader.readLine();


                    if ((message != null))                    //Read string from client side
                    {


                        if (message.contains("Join")) {

                            String[] msg = message.split("##");
                            int port =Integer.parseInt(msg[1]);
                            int avd = port / 2;
                            String hash = genHash(String.valueOf(avd));

                            hmap.put(hash, String.valueOf(avd));
                            hashList.add(hash);
                            Collections.sort(hashList);
                            addClient();
                        }

                        if (message.contains("ADD_ME")) {
                            hashList.clear();
                            int num = Integer.parseInt(reader.readLine());
                            for (int i = 0; i < num; i++) {
                                message = reader.readLine();
                                String[] hampval = message.split("##");
                                hmap.put(hampval[0], hampval[1]);
                                hashList.add(hampval[0]);
                                Collections.sort(hashList);
                            }


                        }




                        if (message.equals("Deletee")) {
                            String fname = reader.readLine();
                            getContext().deleteFile(fname);
                        }
                        if (message.equals("InsertThis")) {
                            String key = reader.readLine();
                            String val = reader.readLine();
                            osw(key, val);

                        }
                        if (message.equals("QueryPresent")) {

                            String filename = reader.readLine();

                            String p = callFis(filename);
                            PrintStream printer = new PrintStream(socket.getOutputStream());             //Write over the socket
                            String query_return = filename + "##" + p;
                            printer.println(query_return);
                            printer.flush();

                        }
                        if (message.equals("All")) {
                            String appPath = getContext().getFilesDir().getPath();
                            File path = new File(appPath);
                            File[] files = path.listFiles();
                            int count = files.length;
                            PrintStream printer = new PrintStream(socket.getOutputStream());
                            printer.println(count);
                            printer.flush();
                            for (File file: files) {
                                if (file.isFile()) {
                                    String filename = file.getName();



                                    String p =  callFis(filename);

                                    printer.println(filename + "##" + p);
                                    printer.flush();
                                }
                            }
                        }


                    }

                } catch (IOException e) {
                    Log.e(TAG, "IO Exception has occurred.");
                    System.out.println(e + " Exception Occurred");
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }

        }
    }
    public Socket createSocket (int port) {
        Socket socket = null;
        byte[] ipAddr = new byte[]{10, 0, 2, 2};
        InetAddress addr = null;
        try {
            addr = InetAddress.getByAddress(ipAddr);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        try {
             socket = new Socket(addr, port );
        } catch (IOException e) {
            e.printStackTrace();
        }
    return socket;
    }

    public String callFis(String filename){
        FileInputStream fis = null;
        String str = null;
        try {
            fis = getContext().openFileInput(filename);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        InputStreamReader isr = new InputStreamReader(fis);
        BufferedReader bufferedReader = new BufferedReader(isr);
        try {
            str =  bufferedReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return str;
    }

    public void addClient(){
        try {

            for(int i=0;i<hashList.size();i++)
            {

                String avd = hmap.get(hashList.get(i));

                if (!(avd.equals("5554"))) {

                   createClientTask("ADD_MEE",avd);
                }
            }

        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public void osw(String key, String value)
    {
        OutputStreamWriter outputStreamWriter = null;
        try {
            outputStreamWriter = new OutputStreamWriter(getContext().openFileOutput(key, Context.MODE_PRIVATE));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            outputStreamWriter.write(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            outputStreamWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class ClientTask extends AsyncTask<String, Void, String> {


        @Override
        protected String doInBackground(String... strings) {

            try {
                String msgToSend = strings[0];

                if(strings[0].contains("JoinReq")) {
                    int main = 11108;

                    Socket socket = createSocket(main);


                    System.out.println("connected to 11108");
                    PrintStream ps = new PrintStream(socket.getOutputStream());

                    //ObjectOutputStream ooStream = new ObjectOutputStream(socket.getOutputStream());
                    String msg = "Join##" + myPortAdress;
//                    ooStream.writeObject(msg);
//                    ooStream.flush();


                    ps.println(msg);
                    Thread.sleep(100);

                }

                Thread.sleep(10);
                Log.e(TAG, "Sent");

                if (msgToSend.contains("ADD_MEE")) {

                    String avd = strings[1];

                    Socket socket1 = createSocket(Integer.parseInt(avd)*2);

                    //ObjectOutputStream ooStream = new ObjectOutputStream(socket1.getOutputStream());

                    PrintStream psc = new PrintStream(socket1.getOutputStream());
                    String msg_client = "ADD_ME";
                    psc.println(msg_client);
                    psc.flush();
                    psc.println(hmap.size());
                    psc.flush();
                    for(String key : hmap.keySet())
                    {
                        String mapinfo = key+"##"+hmap.get(key);
                        System.out.println("Sending MapInfo## "+mapinfo);
                        psc.println(mapinfo);
                        psc.flush();
                    }
                    Thread.sleep(100);
//                    ooStream.writeObject("ADD_ME");
//                    ooStream.flush();
//
//                    ooStream.writeObject(hmap.size());
//                    ooStream.flush();
//
//                    System.out.println("hamp size in ADD_MEE is## "+hmap.size());
//
//                    for (String key ## hmap.keySet()) {
//                        String hmapval = key + "##" + hmap.get(key);
//                        System.out.println("Sending key_value## " + hmapval);
//                        ooStream.writeObject(hmapval);
//                        ooStream.flush();
//                    }
                }



                if (msgToSend.contains("INSERT")) {

                    String[] msg = strings[1].split("##");

                    int port = Integer.parseInt(msg[0]);
                    String avd = msg[0];

                    String key = msg[1];
                    String value = msg[2];
                    Socket socket2 = createSocket(port *2);


                    //  ObjectOutputStream ooStream1 = new ObjectOutputStream(socket2.getOutputStream());
                    PrintStream psc = new PrintStream(socket2.getOutputStream());
                    Thread.sleep(100);
                    String msg_client = "InsertThis";
                    psc.println(msg_client);
                    psc.flush();
                    psc.println(key);
                    psc.flush();
                    psc.println(value);
                    psc.flush();

//                    ooStream1.writeObject(send);
//                    ooStream1.flush();


                }
                if(strings[0].equals("PerformAll"))
                {
                    List<String> tosend = new ArrayList<String>();
                    for(String key : hmap.keySet())
                    {
                        tosend.add(hmap.get(key));
                    }
                    tosend.remove(avd_id);
                    String toreturn="firsttime";
                    for(int j=0;j<tosend.size();j++) {

                        int socket = Integer.parseInt(tosend.get(j))*2;

                        Socket Queryclient = createSocket(socket);


                        PrintStream psq = new PrintStream(Queryclient.getOutputStream());
                        String query_client = "All";
                        psq.println(query_client);
                        psq.flush();
                        Thread.sleep(100);

                        InputStreamReader input = new InputStreamReader(Queryclient.getInputStream());   //Read over the socket
                        BufferedReader reader = new BufferedReader(input);
                        int count = Integer.parseInt(reader.readLine());

                        for (int i = 0; i < count; i++) {
                            String recv = reader.readLine();
                            String[] recvarr = recv.split("##");
                            String fname = recvarr[0];
                            String p_val = recvarr[1];
                            if(toreturn.equals("firsttime"))
                                toreturn = fname + "##" + p_val + "-";
                            else
                                toreturn = toreturn + fname + "##" + p_val + "-";

                        }
                    }
                    return toreturn;
                }
                if(strings[0].equals("Chknext"))
                {

                    String msg_query = strings[1].trim();                                                         //Message to be sent
                    String[] msg_query_array = msg_query.split("##");
                    String presentinavd = msg_query_array[1];
                    String keytoquery = msg_query_array[3];
                    int QueryPort = Integer.parseInt(presentinavd)*2;


                    Socket Queryclient = createSocket(QueryPort);


                    PrintStream psq = new PrintStream(Queryclient.getOutputStream());
                    String query_client = "QueryPresent";
                    psq.println(query_client);
                    psq.flush();
                    psq.println(keytoquery);
                    psq.flush();
                    Thread.sleep(100);
                    InputStreamReader input = new InputStreamReader(Queryclient.getInputStream());   //Read over the socket
                    BufferedReader reader = new BufferedReader(input);
                    String query_return = reader.readLine();
                    String[] query_array = query_return.split("##");
                    String fname = query_array[0];
                    String p_val = query_array[1];
                    return fname+"##"+p_val;

                }
                if(strings[0].equals("Delete"))
                {
                    String[] avd_fname = strings[1].split("##");
                    String avd = avd_fname[0];
                    String file = avd_fname[1];
                    Socket client = createSocket(Integer.parseInt(avd)*2);

                    PrintStream psc = new PrintStream(client.getOutputStream());
                    String msg_client = "Deletee";
                    psc.println(msg_client);
                    psc.flush();
                    psc.println(file);
                    psc.flush();
                }




/*                ObjectInputStream oiStream = new ObjectInputStream(socket.getInputStream());
                String message = oiStream.readObject().toString();
                ObjectOutputStream ooStream = new ObjectOutputStream(socket.getOutputStream());

                if (message.equals("ACK")) {
                    System.out.println("Acknowledgement received from socket.");
                    socket.close();
                    ooStream.close();
                    System.out.println("Socket is closed now.");


                }
*/

            } catch (OptionalDataException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}