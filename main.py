import argparse
import json
import logging

import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    GoogleCloudOptions,
)

# from pipeline.errors import ProcessErrors
from pipeline.process import ProcessEvents


class ProcessingOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_subscription",
            help="The Cloud Pub/Sub subscription\n"
            '"projects/<PROJECT_NAME>/subscriptions/<SUBSCRIPTION_NAME>".',
        )
        parser.add_argument(
            "--window_size", help="Fixed window size in seconds", default=300, type=int
        )
        parser.add_argument("--error_path", help="Path to write errors")
        parser.add_argument(
            "--loc_data_path",
            help="The GCS file location containing device location mapping",
        )
        parser.add_argument(
            "--output_path",
            help="The GCS file location to write the enriched evenets with location mapping",
        )

def print_event(event):
    logging.info(event)
    return event

def run(pipeline_args=None):
    # `save_main_session` is set to true because some DoFn's rely on
    # globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )
    processing_options = pipeline_options.view_as(ProcessingOptions)
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)
    project_id = gcp_options.project
    window_size = processing_options.window_size
    loc_data_path = processing_options.loc_data_path
    output_path = processing_options.output_path

    with beam.Pipeline(options=pipeline_options) as pipeline:
        events = (
            pipeline
            | "Read PubSub Messages"
            >> beam.io.ReadFromPubSub(
                subscription=processing_options.input_subscription
            )
            | "Window into data"
            >> beam.WindowInto(window.FixedWindows(processing_options.window_size))
            | "Convert to dictionary objects" >> beam.Map(json.loads)
            | "print" >> beam.Map(print_event)
        )

        _ = (
            events 
            | "Process Events" >> ProcessEvents(project_id,loc_data_path,window_size,output_path)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    args, pipeline_args = parser.parse_known_args()
    run(pipeline_args)
