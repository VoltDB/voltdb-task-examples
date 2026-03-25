/* This file is part of VoltDB.
 * Copyright (C) 2019-2024 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package voltdb.tasks;

import org.voltdb.client.Client2;
import org.voltdb.client.Client2Config;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdbtest.testcontainer.VoltDBCluster;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base class for VoltDB task integration tests using Testcontainers.
 */
public class IntegrationTestBase {

    private static final Properties props = new Properties();
    static {
        try (InputStream input = IntegrationTestBase.class.getClassLoader()
                .getResourceAsStream("test.properties")) {
            if (input != null) {
                props.load(input);
            }
        } catch (IOException e) {
            // Use defaults
        }
    }

    public String getImageVersion() {
        return props.getProperty("voltdb.image.version", "15.2.0");
    }

    public String getTestMode() {
        return props.getProperty("voltdb.test.mode", "testcontainer");
    }

    public boolean isTestContainerMode() {
        return "testcontainer".equalsIgnoreCase(getTestMode());
    }

    public boolean isShutdownOnCompletion() {
        return Boolean.parseBoolean(
            props.getProperty("voltdb.testcontainer.shutdown", "true"));
    }

    public String getExternalHost() {
        return props.getProperty("voltdb.external.host", "localhost");
    }

    public int getExternalPort() {
        return Integer.parseInt(
            props.getProperty("voltdb.external.port", "21211"));
    }

    /**
     * Creates a VoltDB testcontainer cluster.
     */
    public VoltDBCluster createTestContainer() {
        return new VoltDBCluster(
            getLicensePath(),
            "voltdb/voltdb-enterprise:" + getImageVersion(),
            null  // no extra lib directory needed
        );
    }

    /**
     * Starts the container and loads the task classes from the project JAR.
     */
    public void startAndLoadClasses(VoltDBCluster db) {
        try {
            db.start();
            File jar = getProjectJar();
            if (jar != null && jar.exists()) {
                System.out.println("Loading classes from: " + jar.getAbsolutePath());
                ClientResponse response = db.loadClasses(jar.getAbsolutePath());
                assertEquals(ClientResponse.SUCCESS, response.getStatus(),
                    "Load classes must succeed: " + response.getStatusString());
            } else {
                throw new RuntimeException("Project JAR not found. Run 'mvn package -DskipTests' first.");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to start container and load classes", e);
        }
    }

    /**
     * Creates a client connected to an external VoltDB instance.
     */
    public Client2 createExternalClient() throws Exception {
        Client2Config config = new Client2Config();
        Client2 client = ClientFactory.createClient(config);
        String host = getExternalHost();
        int port = getExternalPort();
        System.out.println("Connecting to external VoltDB at " + host + ":" + port);
        client.connectSync(host, port);
        return client;
    }

    /**
     * Shuts down the testcontainer if configured to do so.
     */
    public void shutdownIfNeeded(VoltDBCluster db) {
        if (db != null && isShutdownOnCompletion()) {
            System.out.println("Shutting down VoltDB testcontainer...");
            db.shutdown();
        } else if (db != null) {
            System.out.println("Keeping VoltDB testcontainer running (shutdown disabled).");
        }
    }

    /**
     * Gets the project JAR path from test.properties or uses default naming.
     */
    protected File getProjectJar() {
        String jarPath = props.getProperty("project.jar.path");
        if (jarPath != null) {
            File jar = new File(jarPath);
            if (jar.exists()) {
                return jar;
            }
        }
        // Try default Maven target directory
        File targetDir = new File("target");
        if (targetDir.exists()) {
            File[] jars = targetDir.listFiles((dir, name) ->
                name.endsWith(".jar") && !name.contains("-sources") && !name.contains("-javadoc"));
            if (jars != null && jars.length > 0) {
                return jars[0];
            }
        }
        return null;
    }

    /**
     * Gets the VoltDB license path from environment or default location.
     */
    protected String getLicensePath() {
        String envLicense = System.getenv("VOLTDB_LICENSE");
        if (envLicense != null) {
            File file = Paths.get(envLicense).toAbsolutePath().toFile();
            if (file.exists()) {
                System.out.println("Using license from VOLTDB_LICENSE: " + file.getAbsolutePath());
                return file.getAbsolutePath();
            }
        }
        String defaultPath = "/tmp/voltdb-license.xml";
        System.out.println("Using default license path: " + defaultPath);
        return defaultPath;
    }
}
