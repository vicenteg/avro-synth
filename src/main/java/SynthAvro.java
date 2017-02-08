import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import com.mapr.synth.samplers.SchemaSampler;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Created by vincegonzalez on 2/7/17. */
public class SynthAvro {
  private final Logger log = LoggerFactory.getLogger(SynthAvro.class);

  public static void main(String[] args)
      throws URISyntaxException, IOException, InterruptedException {
    final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
    ObjectMapper synthMapper = new ObjectMapper();
    AvroMapper mapper = new AvroMapper();
    CommandLineParser parser = new DefaultParser();

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

    long now = System.currentTimeMillis() / 1000L;

    ArrayList<SongStream> songStreams = new ArrayList<SongStream>();

    for (long i = 0; i < count; i++) {
      OutputStream w = Files.newOutputStream(Paths.get("/tmp/avrotest.avro"));
      ObjectWriter ow =  mapper.writer(mapper.schemaFor(SongStream.class));
      JsonNode payload = sampler.sample();
      long sampleTimestamp = payload.get("timestamp").asLong();
      long delayInMillis = (sampleTimestamp - now) * 1000;
      String messageString = payload.toString();
      SongStream ss = synthMapper.treeToValue(payload, SongStream.class);
      ow.writeValue(w, ss);
    }

    System.exit(0);
  }
}
