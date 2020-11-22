import javax.naming.directory.SearchResult;
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataproc.v1.ClusterControllerSettings;
import com.google.cloud.dataproc.v1.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Lists;


import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.paging.Page;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class GUI {
    static JFrame frame = new JFrame("Zach's Search Engine");
    static JPanel panel = new JPanel();
    static JPanel panelChooseAction = new JPanel();
    static JPanel panelSearch = new JPanel();
    static JPanel panelTopNResult = new JPanel();
    static JTextArea searchResult = new JTextArea();
    static JTextArea topNResult = new JTextArea();

    public GUI(){
        final ArrayList<String> fl = new ArrayList<String>();
        //JFrame frame = new JFrame("Zach's Search Engine");
        frame.setBounds(100, 100, 450, 300);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        //panel.setLayout(new GridLayout(4, 0));
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBorder(BorderFactory.createEmptyBorder(30, 50, 10, 50));
        JLabel label = new JLabel("Load My Engine", SwingConstants.CENTER);
        label.setAlignmentX(Label.CENTER_ALIGNMENT);
        final JList fileList = new JList();
        fileList.setSize(30,30);
        JButton btnFileChoose = new JButton("Choose Files");
        btnFileChoose.setAlignmentX(Button.CENTER_ALIGNMENT);
        JButton btnInvertedIndex = new JButton("Construct Inverted Indices");
        btnInvertedIndex.setAlignmentX(Button.CENTER_ALIGNMENT);
//        button1.setFont(new Font("Serif", Font.ITALIC + Font.BOLD, 25));
//        button2.setFont(new Font("Serif", Font.ITALIC + Font.BOLD, 25));
        btnFileChoose.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {

                JFileChooser fc = new JFileChooser();
                fc.setCurrentDirectory(new java.io.File("."));
                fc.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
                fc.setMultiSelectionEnabled(true);
                fc.showOpenDialog(frame);
                for (File f : fc.getSelectedFiles()) {
                    fl.add(f.getName());
                }
                fileList.setListData(fl.toArray());
                fileList.setBackground(new Color(0,0,0,0));

            }
        });
        btnInvertedIndex.addActionListener(new ActionListener() {
            //this is the part that the job is submitted to GCP
            //code was based on info from
            //https://cloud.google.com/dataproc/docs/reference/libraries#setting_up_authentication
            @Override
            public void actionPerformed(ActionEvent ae) {
                String fileURL;
                if(fl.size()<3) {
                    fileURL = "gs://dataproc-staging-us-central1-496024023411-enzjlcvm/Input/" + fl.get(0); //for now I could only process one file at a time
                }else{                                                                                 //or process all three files at the same time
                    fileURL = "gs://dataproc-staging-us-central1-496024023411-enzjlcvm/Input/All";
                }
                //here's where the inverted index job is submitted to GCP
                String projectId = "cloudcomputing-293012";
                String region = "us-central1";
                String clusterName = "cluster-b74d";
                String hadoopFsQuery = "your-hadoop-fs-query";

                String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);


                //Instructions on setting up the authenticate credentials
                //https://cloud.google.com/docs/authentication/production
                //InputStream inputStream = this.getClass().getResourceAsStream("/CloudComputing-91f724764b5e.json");
                InputStream inputStream = null;
                try {
                    inputStream = new FileInputStream("src/main/java/CloudComputing-91f724764b5e.json");
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                GoogleCredentials credentials = null;
                try {
                    credentials = GoogleCredentials.fromStream(inputStream)
                            .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
                Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(),new JacksonFactory(), requestInitializer)
                        .setApplicationName("inverted-index")
                        .build();
                //actually submitting the job
                try {
                    Job submittedJob = dataproc.projects().regions().jobs().submit(
                            projectId, region, new SubmitJobRequest()
                                    .setJob(new Job()
                                            .setPlacement(new JobPlacement()
                                                    .setClusterName(clusterName))
                                            .setHadoopJob(new HadoopJob()
                                                    .setMainClass("InvertedIndex")
                                                    .setJarFileUris(ImmutableList.of("gs://dataproc-staging-us-central1-496024023411-enzjlcvm/InvertedIndex.jar"))
                                                    .setArgs(ImmutableList.of(
                                                            fileURL, "gs://dataproc-staging-us-central1-496024023411-enzjlcvm/Output")))))
                            .execute();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                /*
                //here's the part where the output file is downloaded
                //code is based on info here
                //https://github.com/googleapis/java-dataproc/blob/master/samples/snippets/src/main/java/SubmitHadoopFsJob.java
                try {
                    Storage storage = StorageOptions.newBuilder()
                            .setCredentials(credentials)
                            .setProjectId(projectId)
                            .build()
                            .getService();
                    Bucket bucket = storage.get("dataproc-staging-us-central1-496024023411-enzjlcvm");
                    Page<Blob> blobs = bucket.list(
                            Storage.BlobListOption.prefix("gs://dataproc-staging-us-central1-496024023411-enzjlcvm/Output"));
                    for (Blob blob : blobs.iterateAll()) {
                        String blobContent = new String(blob.getContent());
                        //output.append(blobContent);
                    }
                } catch(Exception err) {
                    err.printStackTrace();
                }
                 */

                //display the page for user to choose action they want
                actionPage();
            }

        });
        panel.add(label);
        panel.add(btnFileChoose);
        panel.add(fileList);
        fileList.setOpaque(false);
        panel.add(btnInvertedIndex);
        frame.add(panel, BorderLayout.CENTER);
        //frame.pack();
        frame.setVisible(true);
    }

    public void actionPage(){
        panel.setVisible(false);
        panel = new JPanel();
        JTextArea text = new JTextArea();
        panel.setLayout(new GridLayout(3, 0));
        panel.setBorder(BorderFactory.createEmptyBorder(30, 50, 10, 50));
        text.append("                              Engine was loaded\n");
        text.append("                                           &\n");
        text.append("      Inverted indicies were constructed successfully!\n");
        text.append("                             Please Select Action\n");
        //text.setBackground(new Color(0,0,0,0));
        text.setOpaque(false);
        //textA.setBounds(210, 100, 700, 220);;
        panel.add(text);
        JButton btnSearch = new JButton("Search for term");
        JButton btnTopN = new JButton("Top-N");
        panel.add(btnSearch);
        panel.add(btnTopN);
        frame.add(panel, BorderLayout.CENTER);
        btnSearch.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                SearchJobInterface();
            }
        });

        btnTopN.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                // do top n here

                TopNInterface();
            }
        });
    }

    public void SearchJobInterface(){
        panel.setVisible(false);
        panel = new JPanel();
        //panelChooseAction.setVisible(false);
        JTextArea text = new JTextArea();
        panel.setLayout(new GridLayout(4,0));
        panel.setBorder(BorderFactory.createEmptyBorder(50, 50, 10, 50));
        panel.setSize(50, 50);
        text.append("                          Enter Your Search Term\n");
        //text.setBackground(new Color(0,0,0,0));
        text.setOpaque(false);
        //textA.setBounds(210, 100, 700, 220);;
        panel.add(text);
        JButton btnSearch = new JButton("Conduct Search");
        JButton btnSearchResult = new JButton("See Search Results");
        final JTextField tfSearch = new JTextField();
        panel.add(tfSearch);
        panel.add(btnSearch);
        panel.add(btnSearchResult);
        frame.add(panel, BorderLayout.CENTER);
        btnSearch.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                // do search job here
                String fileURL = "gs://dataproc-staging-us-central1-496024023411-enzjlcvm/Output";

                //here's where the search job is submitted to GCP
                String projectId = "cloudcomputing-293012";
                String region = "us-central1";
                String clusterName = "cluster-b74d";
                String hadoopFsQuery = "your-hadoop-fs-query";

                String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);

                //Instructions on setting up the authenticate credentials
                //https://cloud.google.com/docs/authentication/production
                //InputStream inputStream = this.getClass().getResourceAsStream("/CloudComputing-91f724764b5e.json");
                InputStream inputStream = null;
                try {
                    inputStream = new FileInputStream("src/main/java/CloudComputing-91f724764b5e.json");
                } catch (FileNotFoundException fe) {
                    fe.printStackTrace();
                }
                GoogleCredentials credentials = null;
                try {
                    credentials = GoogleCredentials.fromStream(inputStream)
                            .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
                HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
                Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(),new JacksonFactory(), requestInitializer)
                        .setApplicationName("searching")
                        .build();
                //actually submitting the job
                try {
                    Job submittedJob = dataproc.projects().regions().jobs().submit(
                            projectId, region, new SubmitJobRequest()
                                    .setJob(new Job()
                                            .setPlacement(new JobPlacement()
                                                    .setClusterName(clusterName))
                                            .setHadoopJob(new HadoopJob()
                                                    .setMainClass("Searching")
                                                    .setJarFileUris(ImmutableList.of("gs://dataproc-staging-us-central1-496024023411-enzjlcvm/Searching.jar"))
                                                    .setArgs(ImmutableList.of(
                                                            fileURL, "gs://dataproc-staging-us-central1-496024023411-enzjlcvm/result", tfSearch.getText())))))
                            .execute();
                } catch (IOException e2) {
                    e2.printStackTrace();
                }

                //searchResult.append("You searched for the term:\n" + tfSearch.getText());
                //here's the part where the output file is downloaded
                //code is based on info here
                //https://github.com/googleapis/java-dataproc/blob/master/samples/snippets/src/main/java/SubmitHadoopFsJob.java
//                GoogleCredentials credentials2 = null;
//                try {
//                    credentials2 = GoogleCredentials.fromStream(inputStream)
//                            .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
//                } catch (IOException ioException) {
//                    ioException.printStackTrace();
//                }
                if(credentials == null){
                    System.out.println("Credentials are null, ERROR");
                }
                try {
                    Storage storage = StorageOptions.newBuilder()
                            .setCredentials(credentials)
                            .build()
                            .getService();
                    Bucket bucket = storage.get("dataproc-staging-us-central1-496024023411-enzjlcvm");
//                    Page<Blob> blobs = bucket.list(
//                            Storage.BlobListOption.prefix("gs://dataproc-staging-us-central1-496024023411-enzjlcvm"));
//                    if(blobs == null){
//                        System.out.println("BLOBS ARE EMPTY, something is wrong");
//                    }
                    Iterable<Blob> blobs = storage.list("dataproc-staging-us-central1-496024023411-enzjlcvm", Storage.BlobListOption.prefix("result")).iterateAll();
                    int i =0;
                    for (Blob blob : blobs) {
//                        String blobContent = new String(blob.getContent());
                        //System.out.println("iterating the blob" + i);
                        //i++;
                        String line;
                        ReadChannel channel = blob.reader();
                        BufferedReader reader = new BufferedReader(Channels.newReader(channel, "UTF-8"));
                        while((line = reader.readLine())!= null){
                            System.out.println(line);
                            searchResult.append(line + "\n");
                        }
                    }
                    System.out.println("Search job is done");
                } catch(Exception err) {
                    err.printStackTrace();
                }
            }
        });
        btnSearchResult.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                DisplaySearchInterface(tfSearch.getText());
            }
        });
    }

    public void TopNInterface(){
        panel.setVisible(false);
        panel = new JPanel();
        JTextArea text = new JTextArea();
        panel.setLayout(new GridLayout(4,0));
        panel.setBorder(BorderFactory.createEmptyBorder(50, 50, 10, 50));
        panel.setSize(50, 50);
        text.append("                             Enter Your N Value\n");
        //text.setBackground(new Color(0,0,0,0));
        text.setOpaque(false);
        panel.add(text);
        JButton btnTopN = new JButton("Conduct Top-N");
        JButton btnTopNResult = new JButton("See Top-N Results");
        final JTextField tfTopN = new JTextField();
        panel.add(tfTopN);
        panel.add(btnTopN);
        panel.add(btnTopNResult);
        frame.add(panel, BorderLayout.CENTER);
        btnTopN.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                // do search job here
                String fileURL = "gs://dataproc-staging-us-central1-496024023411-enzjlcvm/Output";

                //here's where the search job is submitted to GCP
                String projectId = "cloudcomputing-293012";
                String region = "us-central1";
                String clusterName = "cluster-b74d";
                String hadoopFsQuery = "your-hadoop-fs-query";

                String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);

                //Instructions on setting up the authenticate credentials
                //https://cloud.google.com/docs/authentication/production
                //InputStream inputStream = this.getClass().getResourceAsStream("/CloudComputing-91f724764b5e.json");
                InputStream inputStream = null;
                try {
                    inputStream = new FileInputStream("src/main/java/CloudComputing-91f724764b5e.json");
                } catch (FileNotFoundException fe) {
                    fe.printStackTrace();
                }
                GoogleCredentials credentials = null;
                try {
                    credentials = GoogleCredentials.fromStream(inputStream)
                            .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
                HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
                Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(),new JacksonFactory(), requestInitializer)
                        .setApplicationName("topn")
                        .build();
                //actually submitting the job
                try {
                    Job submittedJob = dataproc.projects().regions().jobs().submit(
                            projectId, region, new SubmitJobRequest()
                                    .setJob(new Job()
                                            .setPlacement(new JobPlacement()
                                                    .setClusterName(clusterName))
                                            .setHadoopJob(new HadoopJob()
                                                    .setMainClass("TopN")
                                                    .setJarFileUris(ImmutableList.of("gs://dataproc-staging-us-central1-496024023411-enzjlcvm/TopN.jar"))
                                                    .setArgs(ImmutableList.of(
                                                            fileURL, "gs://dataproc-staging-us-central1-496024023411-enzjlcvm/result2", tfTopN.getText())))))
                            .execute();
                } catch (IOException e2) {
                    e2.printStackTrace();
                }

                //searchResult.append("You searched for the term:\n" + tfSearch.getText());
                //here's the part where the output file is downloaded
                //code is based on info here
                //https://github.com/googleapis/java-dataproc/blob/master/samples/snippets/src/main/java/SubmitHadoopFsJob.java
                try {
                    Storage storage = StorageOptions.newBuilder()
                            .setCredentials(credentials)
                            .build()
                            .getService();
                    Bucket bucket = storage.get("dataproc-staging-us-central1-496024023411-enzjlcvm");
                    Iterable<Blob> blobs = storage.list("dataproc-staging-us-central1-496024023411-enzjlcvm", Storage.BlobListOption.prefix("result2")).iterateAll();
                    int i =0;
                    for (Blob blob : blobs) {
                        //i++;
                        String line;
                        ReadChannel channel = blob.reader();
                        BufferedReader reader = new BufferedReader(Channels.newReader(channel, "UTF-8"));
                        while((line = reader.readLine())!= null){
                            System.out.println(line);
                            topNResult.append(line + "\n");
                        }
                    }
                    System.out.println("TopN is done");
                } catch(Exception err) {
                    err.printStackTrace();
                }
            }
        });
        btnTopNResult.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                DisplayTopNInterface(tfTopN.getText());
            }
        });
    }

    public void DisplaySearchInterface(String term){
        JButton back = new JButton("Back");
        back.setAlignmentX(Button.CENTER_ALIGNMENT);
        JScrollPane scroll = new JScrollPane(searchResult);
        scroll.setAlignmentX(JScrollPane.CENTER_ALIGNMENT);
        scroll.setSize(30, 30);
        searchResult.setLineWrap(true);
        searchResult.setWrapStyleWord(true);
        panel.setVisible(false);
        panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        JTextArea t2 = new JTextArea("You searched for term: " + term);
        t2.setOpaque(false);
        t2.setAlignmentX(TextArea.CENTER_ALIGNMENT);
        panel.setBorder(BorderFactory.createEmptyBorder(50, 50, 10, 50));
        panel.setSize(50, 50);
//        text.append("You searched for the term:\n" + term);
        //text.setBackground(new Color(0,0,0,0));
        searchResult.setOpaque(false);
        //textA.setBounds(210, 100, 700, 220);;
        panel.add(t2);
        panel.add(scroll);
        panel.add(back);
        //search result
        frame.add(panel, BorderLayout.CENTER);
        back.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                actionPage();
            }
        });
    }

    public void DisplayTopNInterface(String value){
        JButton back = new JButton("Back");
        back.setAlignmentX(Button.CENTER_ALIGNMENT);
        JScrollPane scroll = new JScrollPane(topNResult);
        scroll.setAlignmentX(JScrollPane.CENTER_ALIGNMENT);
        scroll.setSize(30, 30);
        topNResult.setLineWrap(true);
        topNResult.setWrapStyleWord(true);
        panel.setVisible(false);
        panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        JTextArea t2 = new JTextArea("Your N value: " + value);
        t2.setOpaque(false);
        t2.setAlignmentX(TextArea.CENTER_ALIGNMENT);
        panel.setBorder(BorderFactory.createEmptyBorder(50, 50, 10, 50));
        panel.setSize(50, 50);
        topNResult.setOpaque(false);
        //textA.setBounds(210, 100, 700, 220);;
        panel.add(t2);
        panel.add(scroll);
        panel.add(back);
        //search result
        frame.add(panel, BorderLayout.CENTER);
        back.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                actionPage();
            }
        });
    }

    public static ArrayList<String> stringToList(String s) {
        return new ArrayList<>(Arrays.asList(s.split(" ")));
    }

    public static void main(String args[]) {
        System.out.println("Search Engine is started");
        new GUI();
    }
}

