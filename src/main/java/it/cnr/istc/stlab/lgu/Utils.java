package it.cnr.istc.stlab.lgu;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FilenameUtils;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

public class Utils {

	public enum Format {
		TTL, NT, NQ, GZ, BZ2, UNKNOWN
	}

	public enum CompressionFormat {
		GZ, BZ2, NO_COMPRESSION
	}

	public final static String[] COMPRESSION_EXTENSIONS = new String[] { "gz", "bz2" };

	public static CompressionFormat getCompressionFormat(String file) {
		if (FilenameUtils.isExtension(file, COMPRESSION_EXTENSIONS)) {
			if (FilenameUtils.getExtension(file).equalsIgnoreCase("bz2")) {
				return CompressionFormat.BZ2;
			} else if (FilenameUtils.getExtension(file).equalsIgnoreCase("gz")) {
				return CompressionFormat.GZ;
			}
		}
		return CompressionFormat.NO_COMPRESSION;
	}

	public static Format getFormat(String file) {

		if (FilenameUtils.getExtension(file).equalsIgnoreCase("ttl")) {
			return Format.TTL;
		} else if (FilenameUtils.getExtension(file).equalsIgnoreCase("nt")) {
			return Format.NT;
		} else if (FilenameUtils.getExtension(file).equalsIgnoreCase("nq")) {
			return Format.NQ;
		}
		return Format.UNKNOWN;
	}

	public static long countNumberOfLines(String filename) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(filename));
		long result = br.lines().count();
		br.close();
		return result;
	}

	public static long countNumberOfLines(Reader r) throws IOException {
		BufferedReader br = new BufferedReader(r);
		long result = br.lines().count();
		br.close();
		return result;
	}

	public static String readFile(String filename) {
		return readFile(filename, false);
	}

	public static String readFile(String filename, boolean keepCR) {
		String result = "";
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(filename)));
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			while (line != null) {
				sb.append(line);
				if (keepCR)
					sb.append('\n');
				line = br.readLine();

			}
			result = sb.toString();
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public static List<String> readFileToList(String filename) {
		List<String> result = new ArrayList<>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(filename)));
			String line;
			while ((line = br.readLine()) != null) {
				result.add(line);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public static String toTextFile(String input, String filePath) throws IOException {

		File f = new File(filePath);
		Writer outputStreamWriter = new OutputStreamWriter(new FileOutputStream(f));

		outputStreamWriter.write(input);
		outputStreamWriter.flush();
		outputStreamWriter.close();

		return f.getAbsolutePath();

	}

	public static boolean deleteFile(String filePath) {
		File file = new File(filePath);
		return file.delete();
	}

	public static List<String> getFilesUnderTreeRec(String filePath) {
		List<String> result = new ArrayList<String>();

		File f = new File(filePath);
		for (File child : f.listFiles()) {
			if (child.isDirectory()) {
				result.addAll(getFilesUnderTreeRec(child.getAbsolutePath()));
			} else {
				result.add(child.getAbsolutePath());
			}
		}

		return result;
	}

	public static List<String> readFileToListString(String filename) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			List<String> result = br.lines().collect(Collectors.toList());
			br.close();
			return result;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new ArrayList<>();
	}

	public static RocksDB openLabelMapDB(String pathDB, int gbCahce) throws RocksDBException {
		System.out.println("Opening " + pathDB);
		Options options = new Options();
		BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
		// table_options.block_size = 16 * 1024;
		tableOptions.setBlockSize(16 * 1024);
		// table_options.cache_index_and_filter_blocks = true;
		tableOptions.setCacheIndexAndFilterBlocks(true);
		// table_options.pin_l0_filter_and_index_blocks_in_cache = true;
		tableOptions.setPinL0FilterAndIndexBlocksInCache(true);
		// https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#block-cache-size
		tableOptions.setBlockCache(new LRUCache(gbCahce * SizeUnit.GB));
		//@f:off
		options.setCreateIfMissing(true)
//			.setWriteBufferSize(512 * SizeUnit.MB)
//			.setMaxWriteBufferNumber(2)
				.setIncreaseParallelism(Runtime.getRuntime().availableProcessors())
				// table options
				.setTableFormatConfig(tableOptions)
				// cf_options.level_compaction_dynamic_level_bytes = true;
				.setLevelCompactionDynamicLevelBytes(true)
				// options.max_background_compactions = 4;
				.setMaxBackgroundCompactions(4)
				// options.max_background_flushes = 2;
				.setMaxBackgroundFlushes(2)
				// options.bytes_per_sync = 1048576;
				.setBytesPerSync(1048576)
				// options.compaction_pri = kMinOverlappingRatio;
				.setCompactionPriority(CompactionPriority.MinOverlappingRatio);
		//@f:on
		options.setMergeOperator(new StringAppendOperator());
		RocksDB db = RocksDB.open(options, pathDB);
		return db;

	}

}
