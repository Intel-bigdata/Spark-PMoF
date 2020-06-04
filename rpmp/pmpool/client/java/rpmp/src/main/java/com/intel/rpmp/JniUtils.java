package com.intel.rpmp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class for JNI related operations. */
public class JniUtils {
  private static final String LIBRARY_NAME = "pmpool_client_jni";
  private static boolean isLoaded = false;
  private static volatile JniUtils INSTANCE;
  private static final Logger LOG = LoggerFactory.getLogger(JniUtils.class);

  public static JniUtils getInstance() throws IOException {
    if (INSTANCE == null) {
      synchronized (JniUtils.class) {
        if (INSTANCE == null) {
          try {
            INSTANCE = new JniUtils();
          } catch (IllegalAccessException ex) {
            throw new IOException("IllegalAccess");
          }
        }
      }
    }

    return INSTANCE;
  }

  private JniUtils() throws IOException, IllegalAccessException {
    try {
      loadLibraryFromJar();
    } catch (IOException ex) {
      System.loadLibrary(LIBRARY_NAME);
    }
  }

  static void loadLibraryFromJar() throws IOException, IllegalAccessException {
    synchronized (JniUtils.class) {
      if (!isLoaded) {
        final String libraryToLoad = System.mapLibraryName(LIBRARY_NAME);
        final File libraryFile =
            moveFileFromJarToTemp(System.getProperty("java.io.tmpdir"), libraryToLoad);
        LOG.info("library path is " + libraryFile.getAbsolutePath());
        System.load(libraryFile.getAbsolutePath());
        isLoaded = true;
      }
    }
  }

  private static File moveFileFromJarToTemp(final String tmpDir, String libraryToLoad)
      throws IOException {
    final File temp = File.createTempFile(tmpDir, libraryToLoad);
    try (final InputStream is =
        JniUtils.class.getClassLoader().getResourceAsStream(libraryToLoad)) {
      if (is == null) {
        throw new FileNotFoundException(libraryToLoad);
      } else {
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    }
    return temp;
  }
}
