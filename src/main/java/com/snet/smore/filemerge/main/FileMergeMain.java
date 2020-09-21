package com.snet.smore.filemerge.main;

import com.snet.smore.common.constant.Constant;
import com.snet.smore.common.constant.FileStatusPrefix;
import com.snet.smore.common.util.EnvManager;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FileMergeMain {
    private static ScheduledExecutorService mainService = Executors.newSingleThreadScheduledExecutor();

    public static void main(String[] args) {
        mainService.scheduleWithFixedDelay(FileMergeMain::merge
                , 1
                , EnvManager.getProperty("merge.interval", 300)
                , TimeUnit.SECONDS);
    }

    private static void merge() {
        String sourceRootStr = EnvManager.getProperty("merge.source.file.dir");
        String targetRootStr = EnvManager.getProperty("merge.target.file.dir");

        Path sourceRoot = Paths.get(sourceRootStr);
        Path targetRoot = Paths.get(targetRootStr);

        try {
            Files.createDirectories(sourceRoot);
            Files.createDirectories(targetRoot);
        } catch (Exception e) {
            log.error("An error occurred while creating directory.", e);
        }

        List<Path> targets = findTargetFiles(sourceRoot);

        if (targets.size() < 1)
            return;

        log.info("To merge target files were found. [{}]", targets.size());

        String targetFileName = Constant.sdf1.format(System.currentTimeMillis())
                + EnvManager.getProperty("merge.target.file.ext", ".conversion");
        Path targetPath = Paths.get(targetRootStr, targetFileName);

        String line;
        int fileCnt = 0;
        int lineCnt;

        try (FileChannel writeChannel = FileChannel.open(targetPath
                , StandardOpenOption.CREATE
                , StandardOpenOption.WRITE
                , StandardOpenOption.TRUNCATE_EXISTING)) {

            for (Path p : targets) {
                try (FileInputStream fis = new FileInputStream(p.toFile());
                     ReadableByteChannel rbc = Channels.newChannel(fis);
                     Reader channelReader = Channels.newReader(rbc, "UTF-8");
                     BufferedReader br = new BufferedReader(channelReader)) {

                    lineCnt = 0;
                    while ((line = br.readLine()) != null) {
                        if (fileCnt > 0 && lineCnt == 0) {
                            lineCnt++;
                            continue;
                        }

                        writeChannel.write(ByteBuffer.wrap(line.getBytes()));
                        writeChannel.write(ByteBuffer.wrap(Constant.LINE_SEPARATOR.getBytes()));
                        lineCnt++;
                    }

                    log.info("[{}] merge completed. [{} / {}]", p, fileCnt + 1, targets.size());
                } catch (Exception e) {
                    log.error("An error while merging file. {}", p, e);
                    return;
                }

                fileCnt++;
                Files.delete(p);
            }

        } catch (Exception e) {
            log.error("An error while writing file. {}", targetPath, e);
        }

        if (fileCnt > 0)
            log.info("Merge File Job was successfully completed. [{}]", fileCnt + " Files");
    }

    private static List<Path> findTargetFiles(Path root) {
        List<Path> files = new ArrayList<>();

        int max = EnvManager.getProperty("merge.source.file.max-count", 300);

        String glob = EnvManager.getProperty("merge.source.file.glob", "*.*");
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + glob);

        try (Stream<Path> pathStream = Files.find(root, Integer.MAX_VALUE,
                (p, a) -> matcher.matches(p.getFileName())
                        && !p.getFileName().toString().startsWith(FileStatusPrefix.COMPLETE.getPrefix())
                        && !p.getFileName().toString().startsWith(FileStatusPrefix.ERROR.getPrefix())
                        && !p.getFileName().toString().startsWith(FileStatusPrefix.TEMP.getPrefix())
                        && !a.isDirectory()
                        && a.isRegularFile())) {
            files = pathStream.limit(max).collect(Collectors.toList());
        } catch (Exception e) {
            log.error("An error occurred while finding source files: {}", e.getMessage());
        }

        return files;
    }
}
