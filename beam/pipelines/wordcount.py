from __future__ import annotations

import re
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText

from common import flink_options


class FormatDoFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        word, count = element
        yield f"{word}: {count}"


def run(argv: list[str] | None = None) -> None:
    options = flink_options(streaming=False, extra_args=argv)
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadInput" >> ReadFromText("/data/input.txt")
            | "ExtractWords" >> beam.FlatMap(lambda line: re.findall(r"[A-Za-z']+", line))
            | "ToLowerCase" >> beam.Map(lambda word: word.lower())
            | "PairWithOne" >> beam.Map(lambda word: (word, 1))
            | "CountWords" >> beam.CombinePerKey(sum)
            | "FormatOutput" >> beam.ParDo(FormatDoFn())
            | "WriteOutput" >> WriteToText("/data/output/wordcount", num_shards=1)
        )


if __name__ == "__main__":
    run()
