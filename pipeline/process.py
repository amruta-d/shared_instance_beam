import json
import logging

import apache_beam as beam
from apache_beam.utils import shared
from apache_beam.io.filesystems import FileSystems
from typing import Dict
from copy import deepcopy
import csv
from apache_beam.io import fileio

class AddLocationData(beam.DoFn):
    """
    Adds location metadata to each event.
    We are using Beam's Shared class to maintain a single instance
    of the location data in the form of a dict object which will be
    shared across multiple threads.
    """

    def __init__(self, loc_data_shared_handle, loc_data_path):
        self.loc_data_shared_handle = loc_data_shared_handle
        self.loc_data_path = loc_data_path

    def GetLocationMetadata(self, event_ts):
        """
        Given the timestamp of the current event gets either
        the cached object of location data dict or refreshes the
        location data dict by reading from the GCS file location
        """

        def ReadLocationMetadataFile():
            # wrapper class needed around Dict since Dict does not support weak references
            class _SideInputContainer(Dict):
                pass

            sideinput_container = _SideInputContainer()
            lines = []
            with FileSystems.open(self.loc_data_path) as f:
                for line in f.readlines():
                    lines.append(line.decode("utf-8"))
                reader = csv.reader(lines)
                next(reader)
                for row in reader:
                    sideinput_container[row[0]] = {
                        "asset_tag": row[1],
                        "location_id": row[2],
                        "device_location_tenant_name": row[3],
                    }
            return sideinput_container

        def LoadLocData():
            # sideinput_path = self.loc_data_path
            # Load the file from Google Cloud Storage
            loc_data_dict = ReadLocationMetadataFile()
            # store the event time stamp in the shared object
            loc_data_dict["timestamp"] = event_ts
            return loc_data_dict

        loc_data = self.loc_data_shared_handle.acquire(LoadLocData)
        # Refresh cache object after a fixed duration (1 hour)
        # The cached object timestamp maintained is the pub/sub event timestamp
        # Here when we acquire the shared object by specfying a new tag
        # Using a new tag will force reloading the dict from the GCS file
        if event_ts - loc_data["timestamp"] >= 3600000000:
            loc_data = self.loc_data_shared_handle.acquire(
                LoadLocData, f"{event_ts}"
            )  # refresh the value
        return loc_data

    def start_bundle(self):
        self.loc_data = None

    def process(self, element, t=beam.DoFn.TimestampParam):

        if self.loc_data is None:
            self.loc_data = self.GetLocationMetadata(t.micros)

        event_copy = deepcopy(element)
        device_id = element["device_id"]
        if device_id is not None and device_id in self.loc_data:
            event_copy.update(self.loc_data[device_id])
        else:
            event_copy["device_tag"] = ""
            event_copy["location_id"] = ""
            event_copy["customer_name"] = ""
        yield event_copy


class ProcessEvents(beam.PTransform):
    """
    Beam Transform to process events
    """

    def __init__(
        self,
        project_id,
        loc_data_path,
        window_size,
        output_path,
        streaming=True,
    ):
        self.streaming = streaming
        self.project_id = project_id
        self.window_size = window_size
        self.loc_data_path = loc_data_path
        self.output_path = output_path

    def expand(self, pcoll):
        loc_data_shared_handle = shared.Shared()
        enriched_events = (
            pcoll
            | "Add location metadata to events"
            >> beam.ParDo(AddLocationData(loc_data_shared_handle, self.loc_data_path))
            |"write to gcs" >> fileio.WriteToFiles(
                f"{self.output_path}",
                file_naming=fileio.default_file_naming(
                    "enriched-events", ".json"
                ),
                shards=1,
                max_writers_per_bundle=0,
            )
        )
