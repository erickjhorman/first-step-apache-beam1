import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DemoTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testDemoTest_FilterCountriesByGivenInitialLetter() {

        List<String> expectedOutput = List.of("Togo", "Tanzania", "Thailand");

        PCollection<String> inputFile = pipeline.apply(Create.of(Arrays.asList("Colombia", "", "Mexico", "Togo", "", "", "Tanzania", "Thailand")));
        PCollection<String> finalResult = inputFile.apply(ParDo.of(new FilterByInitialLetter("T")));

        PAssert.that(finalResult).containsInAnyOrder(expectedOutput);
        pipeline.run();
    }
}