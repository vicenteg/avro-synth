import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import com.mapr.synth.samplers.SchemaSampler;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/** Created by vincegonzalez on 2/7/17. */
public class SynthAvro {
  private final Logger log = LoggerFactory.getLogger(SynthAvro.class);

  public static void main(String[] args)
      throws URISyntaxException, IOException, InterruptedException {
    final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
    AvroMapper mapper = new AvroMapper();
    CommandLineParser parser = new DefaultParser();

    AvroSchemaGenerator gen = new AvroSchemaGenerator();
    mapper.acceptJsonFormatVisitor(SongStream.class, gen);
    AvroSchema schemaWrapper = gen.getGeneratedSchema();

    HelpFormatter formatter = new HelpFormatter();
    Options options = new Options();
    options.addOption("t", "topic", true, "Pub/sub topic to use (must already exist).");
    options.addOption("c", "count", true, "Number of messages to generate.");
    options.addOption(
        "r", "realtime", false, "Set this if you want messages emitted in real-time.");
    options.addOption("f", "forever", false, "Emit messages forever.");
    options.addOption("s", "schema", true, "Path to schema file.");

    String topicName = null;
    String schemaFile = null;
    Long count = 100L;
    Boolean realtime = false;
    Boolean emitForever = false;

    try {
      // parse the command line arguments
      CommandLine line = parser.parse(options, args);

      if (line.hasOption("s")) {
        schemaFile = line.getOptionValue("s");
      } else {
        throw new ParseException("A schema file is required.");
      }

      if (line.hasOption("t")) {
        topicName = line.getOptionValue("t");
      } else {
        throw new ParseException("Topic name is required.");
      }

      if (line.hasOption("c")) {
        count = Long.parseLong(line.getOptionValue("c"));
      }

      if (line.hasOption("r")) {
        realtime = true;
      }

      if (line.hasOption("f")) {
        emitForever = true;
      }
    } catch (ParseException exp) {
      formatter.printHelp("pubsubgen", options);
      System.exit(1);
    }

    String schemaJson = new String(Files.readAllBytes(Paths.get(schemaFile)));
    SchemaSampler sampler = new SchemaSampler(schemaJson);

    DatumWriter<GenericRecord> ssWriter = new GenericDatumWriter<GenericRecord>(schemaWrapper.getAvroSchema());
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(ssWriter);
    dataFileWriter.create(schemaWrapper.getAvroSchema(), new File("/tmp/songstream.avro"));

    FileWriter schemaFileWriter = new FileWriter(new File("/tmp/songstream.avsc"));
    schemaFileWriter.write(schemaWrapper.getAvroSchema().toString(true));
    schemaFileWriter.close();

    for (long i = 0; i < count; i++) {
      GenericRecord songStream = new GenericData.Record(schemaWrapper.getAvroSchema());
      JsonNode payload = sampler.sample();
      songStream.put("userId", payload.get("userId").toString());
      songStream.put("songId", (payload.get("songId")).asInt());
      songStream.put("timestamp", payload.get("timestamp").asInt());

      dataFileWriter.append(songStream);
    }

    dataFileWriter.close();
    System.exit(0);
  }
}
