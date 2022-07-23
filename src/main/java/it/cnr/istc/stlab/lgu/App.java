package it.cnr.istc.stlab.lgu ;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;

public class App {

	private static Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(String[] args){	
		try {
			Configurations configs = new Configurations();
			Configuration config = configs.properties("config.properties");

			RocksDB db = Utils.openLabelMapDB(config.getString("dataset_to_entity_db_path"), 10);

			EntityIndexer ei = new EntityIndexer(db, config.getString("laundromat_folder"), args[0]);
			ei.build();

		} catch (ConfigurationException e) {
			e.printStackTrace();
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
}
