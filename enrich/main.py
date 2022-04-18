import json
import logging
import typing as t

from turbine.runtime import Record, Runtime


def enrich_data(records: t.List[Record]) -> t.List[Record]:
    for record in records:
        # record.value is a JSON string, needs to be loaded to interact with it
        logging.info(f"Got email: {json.loads(record.value)}")

    return records


class App:
    @staticmethod
    async def run(turbine: Runtime):

        logging.basicConfig(level=logging.INFO)

        try:
            # Get remote resource
            resource = await turbine.resources("demopg")

            # Read from remote resource
            records = await resource.records("user_activity")

            # Makes environment variable available to the data-application
            turbine.register_secrets("CLEARBIT_API_KEY")

            # Deploy function with source as input
            enriched = await turbine.process(records, enrich_data)

            # Write results out
            await resource.write(enriched, "user_activity_enriched")

        except ChildProcessError as cpe:
            print(cpe)
        except Exception as e:
            print(e)
