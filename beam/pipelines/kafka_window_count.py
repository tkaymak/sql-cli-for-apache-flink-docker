from __future__ import annotations

import json
import typing

import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka

from common import flink_options, kafka_expansion_service


class FormatWindowCountDoFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        product, count = element
        yield (
            product.encode(),
            json.dumps({
                "product": product,
                "count": count,
                "window_end": window.end.to_utc_datetime().isoformat(),
            }).encode(),
        )


def run(argv: list[str] | None = None) -> None:
    extra_args = ["--experiments=use_deprecated_read"] + (argv or [])
    options = flink_options(streaming=True, extra_args=extra_args)
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromKafka" >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": "kafka:9092",
                    "auto.offset.reset": "earliest",
                    "group.id": "beam-window-count",
                },
                topics=["orders"],
                expansion_service=kafka_expansion_service(),
            )
            # Vereinfachung: Element-Timestamps werden von KafkaIO uebernommen (Processing Time).
            # Es werden keine separaten Event-Timestamps zugewiesen.
            | "ExtractProduct" >> beam.Map(lambda kv: json.loads(kv[1])["product"])
            | "Fixed60sWindow" >> beam.WindowInto(window.FixedWindows(60))
            | "PairWithOne" >> beam.Map(lambda p: (p, 1))
            | "CountPerKey" >> beam.CombinePerKey(sum)
            | "Format" >> beam.ParDo(FormatWindowCountDoFn()).with_output_types(typing.Tuple[bytes, bytes])
            | "WriteToKafka" >> WriteToKafka(
                producer_config={"bootstrap.servers": "kafka:9092"},
                topic="beam_product_counts",
                expansion_service=kafka_expansion_service(),
            )
        )


if __name__ == "__main__":
    run()
