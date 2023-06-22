package challenge_1;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.util.HashMap;

public class JSONToParquet {
    public static void main(String[] args) {

        String inputPath = "\\src\\main\\java\\challenge_1\\challenge_json.json", outputPath = "src\\main\\java\\challenge_1\\challenge_parquet";

        Pipeline pipeline = Pipeline.create();

        Schema schema = BeamCustUtil.getSchema();
        PCollection<GenericRecord> pOutput = pipeline.apply("Reading json file", TextIO.read().from(inputPath)).apply(MapElements.via(new ConvertJSONToGeneric())).setCoder(AvroCoder.of(GenericRecord.class, schema));

        pOutput.apply("Writing to parquet file", FileIO.<GenericRecord>write().via(ParquetIO.sink(schema)).to(outputPath).withNumShards(1).withSuffix(".parquet"));

        pipeline.run();
    }
}

class BeamCustUtil {
    public static Schema getSchema() {
        String SCHEMA_STRING =
                "{\"namespace\": \"training_apache_beam.challenge_1\",\n"
                        + " \"type\": \"record\",\n"
                        + " \"name\": \"ParquetFromJson\",\n"
                        + " \"fields\": [\n"
                        + " {\"name\": \"id\", \"type\": \"int\"},\n"
                        + " {\"name\": \"type\", \"type\": \"string\"},\n"
                        + " {\"name\": \"name\", \"type\": \"string\"},\n"
                        + " {\"name\": \"age\", \"type\": \"int\"},\n"
                        + " {\"name\": \"gender\", \"type\": \"string\"}\n"
                        + " ]\n"
                        + "}";

        return new Schema.Parser().parse(SCHEMA_STRING);
    }
}

class ConvertJSONToGeneric extends SimpleFunction<String, GenericRecord> {

    @Override
    public GenericRecord apply(String input) {

        Schema schema = BeamCustUtil.getSchema();
        GenericRecord record = new GenericData.Record(schema);

        Gson gson = new GsonBuilder().create();
        HashMap<String, Object> parsedMap1 = gson.fromJson(input, HashMap.class);
        HashMap<String, Object> parsedMap2 = gson.fromJson(parsedMap1.get("attributes").toString(), HashMap.class);

        record.put("id", Integer.parseInt(parsedMap1.get("id").toString()));
        record.put("type", parsedMap1.get("type").toString());
        record.put("name", parsedMap2.get("name").toString());
        record.put("age", (int) Float.parseFloat(parsedMap2.get("age").toString()));
        record.put("gender", parsedMap2.get("gender").toString());

        return record;
    }
}