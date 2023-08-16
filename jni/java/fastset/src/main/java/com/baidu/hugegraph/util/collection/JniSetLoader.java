package com.baidu.hugegraph.util.collection;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;

public class JniSetLoader {

    private static String libraryName = "libJniFastSet";

    private enum LibraryState {
        NOT_LOADED,
        LOADING,
        LOADED
    }

    private static AtomicReference<LibraryState> libraryLoaded =
            new AtomicReference<>(LibraryState.NOT_LOADED);

    public static class OSinfo {
        private static String os = System.getProperty("os.name").toLowerCase();

        private OSinfo() {
        }

        public static boolean isLinux() {
            return os.indexOf("linux") >= 0;
        }

        public static boolean isMacOS() {
            return os.indexOf("mac") >= 0 && os.indexOf("os") > 0 && os.indexOf("x") < 0;
        }

        public static boolean isMacOSX() {
            return os.indexOf("mac") >= 0 && os.indexOf("os") > 0 && os.indexOf("x") > 0;
        }

        public static boolean isWindows() {
            return os.indexOf("windows") >= 0;
        }

    }

    public static void loadLibrary() {
        if (libraryLoaded.get() == LibraryState.LOADED) {
            return;
        }
        if (libraryLoaded.compareAndSet(LibraryState.NOT_LOADED,
                LibraryState.LOADING)) {
            System.load(JniSetLoader.extractLibrary(libraryName));
        }

        libraryLoaded.set(LibraryState.LOADED);
    }

    public static String extractLibrary(String fileName) {
        try {
            String suffix = null;
            if (OSinfo.isLinux()) {
                suffix = ".so";
            } else if (OSinfo.isMacOS() || OSinfo.isMacOSX()) {
                suffix = ".dylib";
            } else if (OSinfo.isWindows()) {
                suffix = ".dll";
            }

            File file = File.createTempFile(fileName, suffix);

            if (file.exists()) {
                System.out.println("Temporary file: " + file.getAbsoluteFile().toPath());
                InputStream link = (JniSetLoader.class.getResourceAsStream("/" + fileName + suffix));

                Files.copy(
                        link,
                        file.getAbsoluteFile().toPath(),
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                return file.getAbsoluteFile().toPath().toString();
            }
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
