package transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.List;

public class PipelineDemo {

    public static void main(String[] args) {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        List<String> list = List.of("Colombia", "France", "The United State", "Bolivia","Camerun");

        PCollection<String> countriesCollection = pipeline.apply(Create.of(list));

        PCollection<String> countriesBeginWithC = countriesCollection
                .apply("Filtering By A", ParDo.of(new DoFn<String, String>() {

                    @ProcessElement
                    public void processElement(@Element String elem, ProcessContext c) {
                        if (elem.startsWith("C")) {
                            c.output(elem);
                        }
                    }
                }));

        var countriesBeginWithB = countriesCollection
                .apply("Filtering By B", ParDo.of(new DoFn<String, String>() {

                    @ProcessElement
                    public void processElement(@Element String elem, ProcessContext c) {
                        if (elem.startsWith("B")) {
                            c.output(elem);
                        }
                    }
                }));

        PCollection<String> mergedCollectionWithFlatten = PCollectionList.of(countriesBeginWithC).and(countriesBeginWithB)
                .apply(Flatten.pCollections());

        //PCollection<String> mergedCollectionWithFlatten = collectionList.apply(Flatten.pCollections());

        mergedCollectionWithFlatten.apply(TextIO.write().to("mergepcollections/extractwords")
                .withoutSharding());
        pipeline.run();
    }
}
