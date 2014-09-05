package de.mpii.fsm.util;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class DfsUtils {

    public static Path[] traverse(Path directory, Configuration conf) throws IOException {
        if (directory == null || conf == null) {
            throw new IllegalArgumentException();
        }
        List<Path> result = new LinkedList<Path>();
        FileSystem fs = FileSystem.get(conf);
        traverse(directory, fs, result);
        return result.toArray(new Path[0]);
    }
    
    private static void traverse(Path path, FileSystem fs, List<Path> paths) throws IOException {
        if (fs.isDirectory(path)) {
            for (FileStatus status : fs.listStatus(path)) {
                traverse(status.getPath(), fs, paths);
            }
        } else {
            paths.add(path);
        }
    }

}
