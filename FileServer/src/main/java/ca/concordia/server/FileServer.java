package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileServer {

    private final FileSystemManager fsManager;
    private final int port;

    //This is the serverr level sync
    private final ReentrantReadWriteLock serverLock = new ReentrantReadWriteLock();

    private final ExecutorService clientPool = Executors.newFixedThreadPool(100);

    public FileServer(int port, String fileSystemName, int totalSize) {
        this.fsManager = new FileSystemManager(fileSystemName, totalSize);
        this.port = port;
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server started. Listening on port " + port + "...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientPool.submit(() -> handleClient(clientSocket));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            clientPool.shutdown();
        }
    }

    private void handleClient(Socket clientSocket) {

        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {

            String line;
            while ((line = reader.readLine()) != null) {

                line = line.trim();
                if (line.isEmpty())
                    continue;

                String[] parts = line.split(" ", 3);
                String cmd = parts[0].toUpperCase();

                try {


                    //This sectrion reads the commands

                    if (cmd.equals("READ") || cmd.equals("LIST")) {
                        serverLock.readLock().lock();
                        try {

                            switch (cmd) {

                                case "READ":
                                    if (parts.length != 2) {
                                        writer.println("ERROR: Invalid command");
                                        break;
                                    }
                                    byte[] content = fsManager.readFile(parts[1]);
                                    writer.println(new String(content));
                                    break;

                                case "LIST":
                                    if (parts.length != 1) {
                                        writer.println("ERROR: Invalid command");
                                        break;
                                    }
                                    String[] files = fsManager.listFiles();
                                    writer.println(files.length == 0 ? "EMPTY" : String.join(",", files));
                                    break;
                            }

                        } finally {
                            serverLock.readLock().unlock();
                        }

                        continue;
                    }


                    //This section writes the commands

                    if (cmd.equals("CREATE") || cmd.equals("WRITE") || cmd.equals("DELETE")) {
                        serverLock.writeLock().lock();
                        try {

                            switch (cmd) {

                                case "CREATE":
                                    if (parts.length != 2) {
                                        writer.println("ERROR: Invalid command");
                                        break;
                                    }
                                    fsManager.createFile(parts[1]);
                                    writer.println("SUCCESS: file " + parts[1] + " created");
                                    break;

                                case "WRITE":
                                    if (parts.length < 2) {
                                        writer.println("ERROR: Invalid command");
                                        break;
                                    }
                                    if (parts.length < 3) {
                                        writer.println("ERROR: Usage: WRITE <filename> <text>");
                                        break;
                                    }

                                    String filename = parts[1];
                                    String text = parts[2];

                                    fsManager.writeFile(filename, text.getBytes());
                                    writer.println("SUCCESS: wrote to " + filename);
                                    break;

                                case "DELETE":
                                    if (parts.length != 2) {
                                        writer.println("ERROR: Invalid command");
                                        break;
                                    }
                                    fsManager.deleteFile(parts[1]);
                                    writer.println("SUCCESS: file " + parts[1] + " deleted");
                                    break;
                            }

                        } finally {
                            serverLock.writeLock().unlock();
                        }

                        continue;
                    }


                    //THis section qis the part that quits

                    if (cmd.equals("QUIT") || cmd.equals("EXIT")) {
                        writer.println("SUCCESS: disconnecting");
                        return;
                    }

                    //This is for the unknown command
                    writer.println("ERROR: Invalid command");

                } catch (Exception e) {
                    String msg = e.getMessage();
                    if (msg == null || msg.isBlank())
                        writer.println("ERROR: Internal server error");
                    else
                        writer.println(msg);
                }
            }

        } catch (Exception ignored) {
        } finally {
            try { clientSocket.close(); } catch (Exception ignored) {}
        }
    }
}
