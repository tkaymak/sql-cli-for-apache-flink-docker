from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import typing

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.transforms import trigger, window
from apache_beam.utils.windowed_value import PaneInfoTiming

from common import flink_options, kafka_expansion_service


def to_timestamped(kv: tuple[bytes, bytes]) -> beam.window.TimestampedValue:
    # order_time liegt 0–5 s in der Vergangenheit, daher entstehen echte verspätete Elemente (late data).
    order = json.loads(kv[1])
    dt = datetime.fromisoformat(order["order_time"])
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ts = dt.timestamp()
    return beam.window.TimestampedValue(order["product"], ts)


class FormatWindowTriggerDoFn(beam.DoFn):
    def __init__(self, mode: str) -> None:
        self.mode = mode

    def process(
        self,
        element: tuple[str, int],
        window=beam.DoFn.WindowParam,
        pane=beam.DoFn.PaneInfoParam,
    ):
        product, count = element
        timing_name = PaneInfoTiming.to_string(pane.timing)
        payload = {
            "product": product,
            "count": count,
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            "timing": timing_name,
            "pane_index": pane.index,
            "mode": self.mode,
        }
        yield (product.encode(), json.dumps(payload).encode())


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Beam window triggers example on Flink")
    parser.add_argument(
        "--mode",
        choices=["accumulating", "discarding"],
        default="accumulating",
        help="Window trigger accumulation mode (accumulating or discarding)",
    )
    known_args, remaining_args = parser.parse_known_args(argv)
    mode = known_args.mode

    extra_args = ["--experiments=use_deprecated_read"] + remaining_args
    options = flink_options(streaming=True, extra_args=extra_args)

    with beam.Pipeline(options=options) as p:
        (
            p
            # 'latest': nur neue Bestellungen lesen, damit Early-, On-Time- und Late-Firings live beobachtet werden können.
            | "ReadFromKafka" >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": "kafka:9092",
                    "auto.offset.reset": "latest",
                    "group.id": f"beam-window-triggers-{mode}",
                },
                topics=["orders"],
                expansion_service=kafka_expansion_service(),
            )
            | "ExtractEventTime" >> beam.Map(to_timestamped)
            | "WindowAndTrigger" >> beam.WindowInto(
                window.FixedWindows(120),
                trigger=trigger.AfterWatermark(
                    early=trigger.AfterProcessingTime(60),
                    late=trigger.AfterCount(1)),
                accumulation_mode=(trigger.AccumulationMode.ACCUMULATING if mode == "accumulating"
                                   else trigger.AccumulationMode.DISCARDING),
                allowed_lateness=300)
            | "PairWithOne" >> beam.Map(lambda p: (p, 1))
            | "CountPerKey" >> beam.CombinePerKey(sum)
            | "Format" >> beam.ParDo(FormatWindowTriggerDoFn(mode)).with_output_types(typing.Tuple[bytes, bytes])
            | "WriteToKafka" >> WriteToKafka(
                producer_config={"bootstrap.servers": "kafka:9092"},
                topic="beam_window_triggers",
                expansion_service=kafka_expansion_service(),
            )
        )


if __name__ == "__main__":
    run()
