/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.readonly.mr.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.server.VoldemortConfig;
import voldemort.store.readonly.fetcher.ConfigurableSocketFactory;
import voldemort.utils.ExceptionUtils;
import voldemort.utils.UndefinedPropertyException;
import voldemort.xml.ClusterMapper;
import voldemort.utils.Props;

/**
 * Helper functions for Hadoop
 * 
 * @author jkreps
 * 
 */
public class HadoopUtils {

    private static Logger logger = Logger.getLogger(HadoopUtils.class);

    private static UserGroupInformation currentHadoopUser;
    private static long lastLoginTime = 0;

    public static FileSystem getFileSystem(Props props) {
        if(!props.containsKey("hadoop.job.ugi"))
            throw new RuntimeException("No parameter hadoop.job.ugi set!");
        return getFileSystem(props.getString("hadoop.job.ugi"));
    }

    public static FileSystem getFileSystem(String user) {
        Configuration conf = new Configuration();
        conf.set("hadoop.job.ugi", user);
        try {
            return FileSystem.get(conf);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read the metadata from a hadoop SequenceFile
     * 
     * @param fs The filesystem to read from
     * @param path The file to read from
     * @return The metadata from this file
     */
    public static Map<String, String> getMetadataFromSequenceFile(FileSystem fs, Path path) {
        try {
            Configuration conf = new Configuration();
            conf.setInt("io.file.buffer.size", 4096);
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, new Configuration());
            SequenceFile.Metadata meta = reader.getMetadata();
            reader.close();
            TreeMap<Text, Text> map = meta.getMetadata();
            Map<String, String> values = new HashMap<String, String>();
            for(Map.Entry<Text, Text> entry: map.entrySet())
                values.put(entry.getKey().toString(), entry.getValue().toString());

            return values;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static JsonSchema getSchemaFromPath(Path path) throws IOException {
        return getSchemaFromPath(path.getFileSystem(new Configuration()), path, true);
    }

    /**
     * Pull the schema off of the given file (if it is a file). If it is a
     * directory, then pull schemas off of all subfiles, and check that they are
     * all the same schema. If so, return that schema, otherwise throw an
     * exception
     * 
     * @param fs The filesystem to use
     * @param path The path from which to get the schema
     * @param checkSameSchema boolean flag to check all files in directory for
     *        same schema
     * @return The schema of this file or all its subfiles
     * @throws IOException
     */
    public static JsonSchema getSchemaFromPath(FileSystem fs, Path path, boolean checkSameSchema)
            throws IOException {
        try {
            if(fs.isFile(path)) {
                // this is a normal file, get a schema from it
                Map<String, String> m = HadoopUtils.getMetadataFromSequenceFile(fs, path);
                if(!m.containsKey("value.schema") || !m.containsKey("key.schema"))
                    throw new IllegalArgumentException("No schema found on file " + path.toString());
                return new JsonSchema(JsonTypeDefinition.fromJson(m.get("key.schema")),
                                      JsonTypeDefinition.fromJson(m.get("value.schema")));
            } else {
                FileStatus[] statuses = null;
                if(fs.isDirectory(path)) {
                    // this is a directory, get schemas from all subfiles
                    statuses = fs.listStatus(path);
                } else {
                    // this is wildcard path, get schemas from all matched files
                    statuses = fs.globStatus(path);
                }
                if(statuses == null || statuses.length == 0)
                    throw new IllegalArgumentException("No files found in path pattern "
                                                       + path.toUri().getPath());
                List<JsonSchema> schemas = new ArrayList<JsonSchema>();
                for(FileStatus status: statuses) {
                    if(!HadoopUtils.shouldPathBeIgnored(status.getPath())) {
                        if(!checkSameSchema) {
                            // return first valid schema w/o checking all files
                            return getSchemaFromPath(fs, status.getPath(), checkSameSchema);
                        }
                        schemas.add(getSchemaFromPath(fs, status.getPath(), checkSameSchema));
                    }
                }

                // now check that all the schemas are the same
                if(schemas.size() > 0) {
                    JsonSchema schema = schemas.get(0);
                    for(int i = 1; i < schemas.size(); i++)
                        if(!schema.equals(schemas.get(i)))
                            throw new IllegalArgumentException("The directory "
                                                               + path.toString()
                                                               + " contains heterogenous schemas: found both '"
                                                               + schema.toString() + "' and '"
                                                               + schemas.get(i).toString() + "'.");

                    return schema;
                } else {
                    throw new IllegalArgumentException("No Valid metedata file found for Path:"
                                                       + path.toString());
                }
            }
        } catch(Exception e) {
            logger.error("failed to get metadata from path:" + path);
            throw new RuntimeException(e);
        }
    }

    public static void setPropsInJob(Configuration conf, Props props) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            props.storeFlattened(output);
            conf.set("azkaban.props", new String(output.toByteArray(), "UTF-8"));
        } catch(IOException e) {
            throw new RuntimeException("This is not possible!", e);
        }
    }

    public static Props getPropsFromJob(Configuration conf) {
        String propsString = conf.get("azkaban.props");
        if(propsString == null)
            throw new UndefinedPropertyException("The required property azkaban.props was not found in the Configuration.");
        try {
            ByteArrayInputStream input = new ByteArrayInputStream(propsString.getBytes("UTF-8"));
            Properties properties = new Properties();
            properties.load(input);
            return new Props(properties);
        } catch(IOException e) {
            throw new RuntimeException("This is not possible!", e);
        }
    }

    public static Cluster readCluster(String clusterFile, Configuration conf) throws IOException {
        return new ClusterMapper().readCluster(new StringReader(readAsString(new Path(clusterFile))));
    }

    /**
     * Find a jar that contains a class of the same name, if any. It will return
     * a jar file, even if that is not the first thing on the class path that
     * has a class with the same name.
     * 
     * @param my_class the class to find.
     * @return a jar file that contains the class, or null.
     * @throws IOException
     */
    public static String findContainingJar(Class my_class, ClassLoader loader) {
        String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
        return findContainingJar(class_file, loader);
    }

    public static String findContainingJar(String fileName, ClassLoader loader) {
        try {
            for(Enumeration itr = loader.getResources(fileName); itr.hasMoreElements();) {
                URL url = (URL) itr.nextElement();
                logger.info("findContainingJar finds url:" + url);
                if("jar".equals(url.getProtocol())) {
                    String toReturn = url.getPath();
                    if(toReturn.startsWith("file:")) {
                        toReturn = toReturn.substring("file:".length());
                    }
                    toReturn = URLDecoder.decode(toReturn, "UTF-8");
                    return toReturn.replaceAll("!.*$", "");
                }
            }
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static FileSystem getFileSystem(String hdfsUrl, boolean isLocal) throws IOException {
        // Initialize fs
        FileSystem fs;
        if(isLocal) {
            fs = FileSystem.getLocal(new Configuration());
        } else {
            fs = new DistributedFileSystem();
            try {
                fs.initialize(new URI(hdfsUrl), new Configuration());
            } catch(URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }
        return fs;
    }

    public static JobConf addAllSubPaths(JobConf conf, Path path) throws IOException {
        if(shouldPathBeIgnored(path)) {
            throw new IllegalArgumentException(String.format("Path[%s] should be ignored.", path));
        }

        final FileSystem fs = path.getFileSystem(conf);

        if(fs.exists(path)) {
            for(FileStatus status: fs.listStatus(path)) {
                if(!shouldPathBeIgnored(status.getPath())) {
                    if(status.isDir()) {
                        addAllSubPaths(conf, status.getPath());
                    } else {
                        FileInputFormat.addInputPath(conf, status.getPath());
                    }
                }
            }
        }

        return conf;
    }

    /**
     * Check if the path should be ignored. Currently only paths with "_log" are
     * ignored.
     * 
     * @param path
     * @return
     * @throws IOException
     */
    public static boolean shouldPathBeIgnored(Path path) throws IOException {
        return path.getName().startsWith("_");
    }

    public static String readAsString(Path path) {
        InputStream input = null;
        try {
            FileSystem fs = path.getFileSystem(new Configuration());
            input = fs.open(path);
            return IOUtils.toString(input);
        } catch(IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(input);
        }
    }

    public static boolean mkdirs(String pathName) throws IOException {
        Path path = new Path(pathName);
        FileSystem fs = path.getFileSystem(new Configuration());
        return fs.mkdirs(path);
    }

    public static void deletePathIfExists(JobConf conf, String stepOutputPath) throws IOException {
        Path path = new Path(stepOutputPath);
        FileSystem fs = path.getFileSystem(conf);
        if(fs.exists(path)) {
            fs.delete(path, true);
        }
    }

    /**
     * Looks for the latest (the alphabetically greatest) path contained in the
     * given directory that passes the specified regex pattern.
     * 
     * @param fs The file system
     * @param directory The directory that will contain the versions
     * @param acceptRegex The String pattern
     * @return
     * @throws IOException
     */
    public static Path getLatestVersionedPath(FileSystem fs, Path directory, String acceptRegex)
            throws IOException {
        final String pattern = acceptRegex != null ? acceptRegex : "\\S+";

        PathFilter filter = new PathFilter() {

            @Override
            public boolean accept(Path arg0) {
                return !arg0.getName().startsWith("_") && Pattern.matches(pattern, arg0.getName());
            }
        };

        FileStatus[] statuses = fs.listStatus(directory, filter);

        if(statuses == null || statuses.length == 0) {
            return null;
        }

        Arrays.sort(statuses);

        return statuses[statuses.length - 1].getPath();
    }

    public static Path getSanitizedPath(Path path) throws IOException {
        return getSanitizedPath(path.getFileSystem(new Configuration()), path);
    }

    /**
     * Does the same thing as getLatestVersionedPath, but checks to see if the
     * directory contains #LATEST. If it doesn't, it just returns what was
     * passed in.
     * 
     * @param fs
     * @param directory
     * @return
     * @throws IOException
     */
    public static Path getSanitizedPath(FileSystem fs, Path directory) throws IOException {
        if(directory.getName().endsWith("#LATEST")) {
            // getparent strips out #LATEST
            return getLatestVersionedPath(fs, directory.getParent(), null);
        }

        return directory;
    }

    /**
     * Move the file from one place to another. Unlike the raw Hadoop API this
     * will throw an exception if it fails. Like the Hadoop api it will fail if
     * a file exists in the destination.
     * 
     * @param fs The filesystem
     * @param from The source file to move
     * @param to The destination location
     * @throws IOException
     */
    public static void move(FileSystem fs, Path from, Path to) throws IOException {
        boolean success = fs.rename(from, to);
        if(!success)
            throw new RuntimeException("Failed to move " + from + " to " + to);
    }

    private static Configuration getConfiguration(VoldemortConfig voldemortConfig, String sourceFileUrl) {
        final Configuration hadoopConfig = new Configuration();
        hadoopConfig.setInt(ConfigurableSocketFactory.SO_RCVBUF, voldemortConfig.getFetcherBufferSize());
        hadoopConfig.setInt(ConfigurableSocketFactory.SO_TIMEOUT, voldemortConfig.getFetcherSocketTimeout());
        hadoopConfig.set("hadoop.rpc.socket.factory.class.ClientProtocol",
                   ConfigurableSocketFactory.class.getName());
        hadoopConfig.set("hadoop.security.group.mapping",
                   "org.apache.hadoop.security.ShellBasedUnixGroupsMapping");

        String hadoopConfigPath = voldemortConfig.getHadoopConfigPath();
        boolean isHftpBasedFetch = sourceFileUrl.length() > 4 &&
                sourceFileUrl.substring(0, 4).equals("hftp");
        logger.info("URL : " + sourceFileUrl + " and hftp protocol enabled = " + isHftpBasedFetch);
        logger.info("Hadoop path = " + hadoopConfigPath + " , keytab path = "
                            + voldemortConfig.getReadOnlyKeytabPath() + " , kerberos principal = "
                            + voldemortConfig.getReadOnlyKerberosUser());

        if(hadoopConfigPath.length() > 0) {

            hadoopConfig.addResource(new Path(hadoopConfigPath + "/core-site.xml"));
            hadoopConfig.addResource(new Path(hadoopConfigPath + "/hdfs-site.xml"));

            String security = hadoopConfig.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION);

            if (security != null && security.equals("kerberos")) {
                logger.info("Kerberos authentication is turned on in the Hadoop conf.");
            } else if (security != null && security.equals("simple")) {
                logger.info("Authentication is explicitly disabled in the Hadoop conf.");
            } else {
                throw new VoldemortException("Error in getting a valid Hadoop Configuration. " +
                                                     "Make sure the Hadoop config directory path is correct via" +
                                                     VoldemortConfig.HADOOP_CONFIG_PATH + " and that the " +
                                                     CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION +
                                                     " property in the Hadoop config is set to either 'kerberos' or 'simple'. " +
                                                     "That property is currently set to '" + security + "'.");
            }
        }
        return hadoopConfig;
    }

    public static FileSystem getHadoopFileSystem(VoldemortConfig voldemortConfig, String sourceFileUrl)
            throws Exception {
        final Configuration config = getConfiguration(voldemortConfig, sourceFileUrl);
        final Path path = new Path(sourceFileUrl);
        final int maxAttempts = voldemortConfig.getReadOnlyFetchRetryCount() + 1;
        final String keytabPath = voldemortConfig.getReadOnlyKeytabPath();
        FileSystem fs = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                if (keytabPath.length() > 0) {
                    // UserGroupInformation.loginUserFromKeytab() should only need to happen once during
                    // the lifetime of the JVM. We try to minimize login operations as much as possible,
                    // but we will redo it if an AuthenticationException is caught below.
                    synchronized (HadoopUtils.class) {
                        long timeSinceLastLogin = System.currentTimeMillis() - lastLoginTime;
                        // The null check within the synchronized block is for two reasons:
                        // 1- To minimize the amount of login operations from concurrent pushes.
                        // 2- To prevent NPEs if the currentHadoopUser is reset to null in the catch block.
                        if (currentHadoopUser == null || timeSinceLastLogin > voldemortConfig.getReadOnlyLoginIntervalMs()) {
                            if (!new File(keytabPath).exists()) {
                                logger.error("Invalid keytab file path. Please provide a valid keytab path");
                                throw new VoldemortException("Error in getting Hadoop filesystem. Invalid keytab file path.");
                            }
                            UserGroupInformation.setConfiguration(config);
                            UserGroupInformation.loginUserFromKeytab(voldemortConfig.getReadOnlyKerberosUser(), keytabPath);
                            currentHadoopUser = UserGroupInformation.getCurrentUser();
                            lastLoginTime = System.currentTimeMillis();
                            logger.info("I have logged in as " + currentHadoopUser.getUserName());
                        } else {
                            // reloginFromKeytab() will not actually do anything unless the token is close to expiring.
                            currentHadoopUser.reloginFromKeytab();
                        }
                    }
                }

                fs = path.getFileSystem(config);

                // Just a small operation to make sure the FileSystem instance works.
                fs.exists(path);

                // Congrats for making it this far. Pass go and collect $200.
                break;
            } catch(VoldemortException e) {
                // We only intend to catch and retry Hadoop-related exceptions, not Voldemort ones.
                throw e;
            } catch(Exception e) {
                if (ExceptionUtils.recursiveClassEquals(e, AuthenticationException.class)) {
                    logger.info("Got an AuthenticationException from HDFS. " +
                                        "Will retry to login from scratch, on next attempt.", e);
                    synchronized (HadoopUtils.class) {
                        // Synchronized to prevent NPEs in the other synchronized block, above.
                        currentHadoopUser = null;
                    }
                }
                if(attempt < maxAttempts) {
                    // We may need to sleep
                    long retryDelayMs = voldemortConfig.getReadOnlyFetchRetryDelayMs();
                    if (retryDelayMs > 0) {
                        // Doing random back off so that all nodes do not end up swarming the KDC infra
                        long randomDelay = (long) (Math.random() * retryDelayMs + retryDelayMs);

                        logger.error("Could not get a valid Filesystem object on attempt # " + attempt +
                                             " / " + maxAttempts + ". Trying again in " + randomDelay + " ms.");
                        try {
                            Thread.sleep(retryDelayMs);
                        } catch(InterruptedException ie) {
                            logger.error("Fetcher interrupted while waiting to retry", ie);
                            Thread.currentThread().interrupt();
                        }
                    }
                } else {
                    throw e;
                }
            }
        }
        return fs;
    }

}
