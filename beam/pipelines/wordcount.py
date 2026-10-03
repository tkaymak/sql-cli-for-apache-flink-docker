from __future__ import annotations

import os
import re
import apache_beam as beam
from apache_beam.io import ReadFromText

from common import flink_options


# Bekannter Bug in Beam 2.76.0 mit dem Flink 2.x Runner: WriteToText schlägt in Batch-Pipelines
# im Finalize-Schritt fehl mit 'IllegalStateException: TimestampCombiner moved element from ...
# (TIMESTAMP_MAX_VALUE) to earlier time ... (end of global window)'.
# Daher ersetzt write_lines() zusammen mit ToList() hier WriteToText.
def write_lines(lines: list[str]) -> int:
    os.makedirs("/data/output", exist_ok=True)
    sorted_lines = sorted(lines)
    with open("/data/output/wordcount.txt", "w", encoding="utf-8") as f:
        for line in sorted_lines:
            f.write(f"{line}\n")
    return len(lines)


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
            | "FormatOutput" >> beam.MapTuple(lambda word, count: f"{word}: {count}")
            | "ToList" >> beam.combiners.ToList()
            | "WriteLines" >> beam.Map(write_lines)
            | "PrintCount" >> beam.Map(print)
        )


if __name__ == "__main__":
    run()
