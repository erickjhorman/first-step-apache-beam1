package transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;


public class CustomLogger {
    public static <T>PTransform<PCollection<T>,PCollection<T>> ofElements(){
        return new LoggingTransform<>();
    }

    private static class LoggingTransform<T> extends PTransform<PCollection<T>, PCollection<T>> {

        @Override
        public PCollection<T> expand(PCollection<T> input) {
            return input.apply(ParDo.of(new DoFn<T, T>() {

                @ProcessElement
                public void processElement(@Element T element, OutputReceiver<T> out, BoundedWindow window) {
                    String message = element.toString();
                    if(!(window instanceof GlobalWindow)) {
                        message = message + " Window:" + window.toString();
                    }
                    System.out.println(message);
                    out.output(element);
                }
            }));
        }
    }
}