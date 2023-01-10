package com.example.training;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TrainingApplication {

	private static final String COMMA_DELIMITER = ",";

	private static final String tableSpec = "us-gcp-ame-con-116-npd-1:training_apache_z.username";

	private static final String project = "my-project-id";
	private static final String dataset = "my_bigquery_dataset_id";
	private static final String table = "my_bigquery_table_id";


	public static void main(String[] args) {

		//1.Load data as row from Bigquery table into a Pcollection - CSV Assignment LOCAL - STARTS

		//Pipeline pipeline = Pipeline.create();

		/*PCollection<String> rows =
				pipeline
						.apply(
								"Read from BigQuery query",
								BigQueryIO.readTableRows()
										.fromQuery(String.format("SELECT * FROM `%s.%s.%s`", project, dataset, table))
										.usingStandardSql())
						.apply(
								"TableRows to MyData",
								MapElements.into(TypeDescriptor.of(String.class)).via(String::fromTableRow));*/

		/*PCollection<Double> rows =
				pipeline.apply(
						BigQueryIO.read(
										(SchemaAndRecord elem) -> (Double) elem.getRecord().get("Username"))
								.fromQuery(
										"SELECT * FROM `us-gcp-ame-con-116-npd-1:training_apache_z.username`")
								.usingStandardSql()
								.withCoder(DoubleCoder.of()));

		PCollection<String> pOutput = pipeline.apply(Create.empty(StringUtf8Coder.of()));
		pOutput.apply(TextIO.write().to("/Users/zjafri/Desktop/DataBifrost_Training/training/output/Output_Sample_BQuery.csv").withHeader("Username,Identifier,First_Name,Last_Name,HashCode").withNumShards(1).withSuffix(".csv"));
*/

		/*PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
		Pipeline pipeline = Pipeline.create(pipelineOptions);

		List<String> inputList = new ArrayList<String>();

		PCollection<String> lines = pipeline.apply(
				"ReadMyFile", TextIO.read().from("/Users/zjafri/Desktop/DataBifrost_Training/training/src/main/resources/username.csv"));

		PCollection<String > pOutput= lines.apply(ParDo.of(new LinesFilter()));
		pOutput.apply(TextIO.write().to("/Users/zjafri/Desktop/DataBifrost_Training/training/output/Output_username_FilteredLeaves.csv").withHeader("Username,Identifier,First_Name,Last_Name,HashCode").withNumShards(1).withSuffix(".csv"));

		pipeline.run().waitUntilFinish();*/

		//1.Load data as row from Bigquery table into a Pcollection - CSV Assignment LOCAL - END


		DataflowPipelineOptions option = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		option.setProject("us-gcp-ame-con-116-npd-1");
		option.setStagingLocation("gs://apache_training_z_bucket/temp");
		option.setRegion("us-central1");
		option.setGcpTempLocation("gs://apache_training_z_bucket/temp1");
		option.setTempLocation("gs://apache_training_z_bucket/temp1");
		option.setNumWorkers(1);
		option.setUsePublicIps(false);
		option.setRunner(DataflowRunner.class);

		Pipeline pipeline = Pipeline.create(option);

		String project = "us-gcp-ame-con-116-npd-1";
		String dataset = "training_apache_z";
		String table = "username";
		String newTable = "username_hashcode_output";

		TableReference tableReference = new TableReference()
				.setProjectId(project)
				.setDatasetId(dataset)
				.setTableId(table);

		TableReference tableReferenceoutput = new TableReference()
				.setProjectId(project)
				.setDatasetId(dataset)
				.setTableId(newTable);

		PCollection<Data> row = pipeline.apply("Read from BQ",
						BigQueryIO.readTableRows().from(tableReference))
				.apply("Convert to row",
						MapElements.into(TypeDescriptor.of(Data.class)).via(Data::fromTableRow));

		List<TableFieldSchema> columns = new ArrayList<TableFieldSchema>();
		columns.add(new TableFieldSchema().setName("Username").setType("STRING").setMode("REQUIRED"));
		columns.add(new TableFieldSchema().setName("Identifier").setType("STRING").setMode("REQUIRED"));
		columns.add(new TableFieldSchema().setName("First_Name").setType("STRING").setMode("REQUIRED"));
		columns.add(new TableFieldSchema().setName("Last_Name").setType("STRING").setMode("REQUIRED"));
		columns.add(new TableFieldSchema().setName("hashcode").setType("STRING").setMode("REQUIRED"));

		TableSchema schema = new TableSchema().setFields(columns);


		row.apply(MapElements.into(TypeDescriptor.of(TableRow.class))
						.via((Data data) -> new TableRow()
								.set("Username", data.username)
								.set("Identifier", data.identifier)
								.set("First_Name", data.first_name)
								.set("Last_Name", data.last_name)
								.set("hashcode", data.username.hashCode()+ data.identifier.hashCode())))

				.apply("Writing Data with Hashcode to Bigquery Table",BigQueryIO.writeTableRows()
						.to(tableReferenceoutput)
						.withSchema(schema)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

		pipeline.run().waitUntilFinish();

	}

}
