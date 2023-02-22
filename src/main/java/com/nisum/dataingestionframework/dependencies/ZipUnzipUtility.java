package com.nisum.dataingestionframework.dependencies;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/* @author Hari Chandra Prasad Nimmagadda */

public class ZipUnzipUtility implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ZipUnzipUtility.class);

    private List<String> fileList;
    private static final String NOTES_ZIP = "D:\\Notes.zip";
    private static final String FOLDER_TO_ZIP = "D:\\Notes";

    public static void unzip(String zipFile, String outputPath){

        if(outputPath == null)
            outputPath = "";
        else
            outputPath+=File.separator;

        // 1.0 Create output directory
        File outputDirectory = new File(outputPath);

        if(outputDirectory.exists())
            outputDirectory.delete();

        outputDirectory.mkdir();


        // 2.0 Unzip (create folders & copy files)
        try {

            // 2.1 Get zip input stream
            ZipInputStream zip = new ZipInputStream(new FileInputStream(zipFile));

            ZipEntry entry = null;
            int len;
            byte[] buffer = new byte[1024];

            // 2.2 Go over each entry "file/folder" in zip file
            while((entry = zip.getNextEntry()) != null){

                if(!entry.isDirectory()){
                    logger.info("-"+entry.getName());

                    // create a new file
                    File file = new File(outputPath +entry.getName());

                    // create file parent directory if does not exist
                    if(!new File(file.getParent()).exists())
                        new File(file.getParent()).mkdirs();

                    // get new file output stream
                    FileOutputStream fos = new FileOutputStream(file);

                    // copy bytes
                    while ((len = zip.read(buffer)) > 0) {
                        fos.write(buffer, 0, len);
                    }

                    fos.close();
                }

            }

        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //  zip(FOLDER_TO_ZIP,NOTES_ZIP);
    //public static void zip(final String sourcNoteseDirPath, final String zipFilePath) throws IOException {
    public void zip(final String sourcNoteseDirPath, final String zipFilePath) throws IOException {
        Path zipFile = Files.createFile(Paths.get(zipFilePath));

        Path sourceDirPath = Paths.get(sourcNoteseDirPath);
        try (ZipOutputStream zipOutputStream = new ZipOutputStream(Files.newOutputStream(zipFile));
             Stream<Path> paths = Files.walk(sourceDirPath)) {
            paths
                    .filter(path -> !Files.isDirectory(path))
                    .forEach(path -> {
                        ZipEntry zipEntry = new ZipEntry(sourceDirPath.relativize(path).toString());
                        try {
                            zipOutputStream.putNextEntry(zipEntry);
                            Files.copy(path, zipOutputStream);
                            zipOutputStream.closeEntry();
                        } catch (IOException e) {
                            System.err.println(e);
                        }
                    });
        }

        logger.info("Zip is created at : "+zipFile);
    }



}
