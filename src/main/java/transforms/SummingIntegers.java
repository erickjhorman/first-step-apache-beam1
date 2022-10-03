package transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class SummingIntegers {

    public static void main(String[] args) {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Reading from file", TextIO.read().from(options.getInput()))
                .apply("Split countries from file", FlatMapElements.into(TypeDescriptors.strings())
                        .via((String line) -> Arrays.asList(line.split(","))))
                .apply(CustomLogger.ofElements())
                .apply("Converting to Integers", MapElements.into(TypeDescriptors.integers())
                        .via(Integer::parseInt))
                .apply("Summing integers", Combine.globally(Sum.ofIntegers()))
                .apply("Converting to String", MapElements
                        .into(TypeDescriptors.strings())
                        .via(Object::toString))
                .apply("Writing final result", TextIO.write()
                        .to(options.getOutput())
                        .withSuffix(".txt")
                        .withoutSharding());

        pipeline.run();
    }

    public interface Options extends PipelineOptions {

        @Description("Input file to be read")
        @Validation.Required
        String getInput();
        void setInput(String input);

        @Description("Output file to save the final result")
        @Default.String("output")
        String getOutput();
        void setOutput(String output);

    }
}