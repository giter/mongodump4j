package me.giter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.bson.io.BasicOutputBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.MongoClient;

public class Mongodump {

  static Logger LOGGER = LoggerFactory.getLogger(Mongodump.class);

  @Parameter(names = { "-h", "--host" }, required = false, description = "Host of mongo instance")
  public String host = "127.0.0.1";

  @Parameter(names = { "-p", "--port" }, required = false, description = "Port of mongo instance")
  public int port = 27017;

  @Parameter(names = { "-d", "--dir" }, required = true, description = "directory backup to")
  public String directory;

  @Parameter(names = { "-b", "--batch" }, required = false, description = "query batch size")
  public int batchSize = 4 * 1024 * 1024;

  @Parameter(names = { "-l", "--limit" }, required = false, description = "query limit")
  public int limit = 10000;

  @Parameter(names = { "--skip" }, required = false, description = "skip db or collections")
  public Set<String> skips = new LinkedHashSet<>();

  @SuppressWarnings("deprecation")
  public static void main(String[] args) throws FileNotFoundException,
      IOException {

    Mongodump options = new Mongodump();

    JCommander commander = new JCommander(options);
    commander.setProgramName("java -jar mongodump.jar");

    try {
      commander.parse(args);
    } catch (ParameterException e) {

      System.err.println("Error: " + e.getMessage());

      commander.usage();
      System.exit(1);
    }

    try (MongoClient mc = new MongoClient(options.host, options.port)) {

      File file = new File(options.directory);
      file.mkdirs();

      for (String dbName : mc.getDatabaseNames()) {

        if (dbName.equals("local"))
          continue;

        if (dbName.equals("admin"))
          continue;

        if (options.skips.contains(dbName))
          continue;

        DB db = mc.getDB(dbName);
        for (String collName : db.getCollectionNames()) {

          if (options.skips.contains(dbName + "." + collName))
            continue;

          DBCollection coll = db.getCollection(collName);

          try (SnappyOutputStream output = new SnappyOutputStream(
              new FileOutputStream(options.directory + "/" + dbName + "#"
                  + collName + ".snappy"))) {

            long n = coll.count();

            DefaultDBEncoder de = new DefaultDBEncoder();
            BasicOutputBuffer ob = new BasicOutputBuffer();
            de.set(ob);

            BasicDBList indexes = new BasicDBList();

            for (DBObject b : coll.getIndexInfo()) {
              indexes.add(b);
            }

            de.putObject(indexes);
            ob.pipe(output);

            LOGGER.info("dumping " + dbName + "." + collName + "[0/" + n
                + "] ...");

            int pc = 0;
            boolean last = true;

            Object _id = null;
            DBObject sort = BasicDBObjectBuilder.start("_id", 1).get();

            for (DBObject o : coll.find().sort(sort).limit(1)) {

              pc++;
              de.putObject(o);
              last = false;
              _id = o.get("_id");
            }

            do {

              last = true;

              for (DBObject o : coll
                  .find(
                      BasicDBObjectBuilder.start().push("_id").add("$gt", _id)
                          .get()).sort(sort).batchSize(options.batchSize)
                  .limit(options.limit)) {

                de.putObject(o);
                pc++;
                last = false;
                _id = o.get("_id");
              }

              if (!last) {

                ob.pipe(output);
              }

              LOGGER.info("dumping " + dbName + "." + collName + "[" + pc + "/"
                  + n + "] ...");

            } while (!last);
          }
        }
      }
    }
  }
}
