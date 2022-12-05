import os
import argparse
from datetime import date

from gcs_utils import json_to_gcs, call_odata_api


def unnest_violations(nested_data, name_swaps):
    """
            De-nests data from the CX API's DOMI/PLI/ENV SVCS Violations dataset. Takes in raw input and from the
            API, which contains several nested rows, and extracts them duplicating data that needs to be present for
            each
            unnested row. This type of operation would normally be done in dataflow. However, the parallel processing
            inherent to Dataflow's functionality, as well as the nature of pipeline fusion was causing
            concurrency/parallelization issues. This represented the most straightforward solution.

            Column names are changed in this function. Normally this would happen in dataflow, but since they have to
             be created de novo either way, it makes more sense to go ahead and format is needed

            :param nested_data: (list of dicts): the raw data from the CX API
            :param name_swaps: (list of list of strings): each sub-list contains the string value names for the fields
            that were not nested. The first list being the old names and the second being the new names
            :param old_nested_keys: (list of strings): The old names of the nested fields
            :param new_unnested_keys: (list of strings): The new names of the unnested fields
            closure segments are dropped or returned

            :return: unnested (list of dicsts): unnested data
            closure segments

    """
    unnested = []
    print("unnesting")

    for row in nested_data:
        new_row_base = {}
        if not row["INVESTIGATION"]:
            continue

        # extract (and rename) all the unnested fields
        for n in range(len(name_swaps[0])):
            new_row_base.update({name_swaps[1][n]: row[name_swaps[0][n]]})

        # for each violation make a copy of the base row, extract the violations, then all of the inspections. each
        # of these operations produces a single dict which is then appended to a list
        for v in row["VIOLATION"]:
            new_row_vio = new_row_base.copy()

            new_row_vio["violation_code_sec"] = v["CODESECTION"]
            new_row_vio["violation_code_title"] = v["CODESECTIONTITLE"]
            new_row_vio["violation_desc"] = v["DESCRIPTION"]
            new_row_vio["violation_spec_instructions"] = v["SPECIALINSTRUCTIONS"]

            for i in row["INVESTIGATION"]:
                new_row_inv = new_row_vio.copy()
                new_row_inv["investigation_date"] = i["DATECOMPLETED"]
                new_row_inv["investigation_outcome"] = i["OUTCOME"]
                new_row_inv["investigation_findings"] = i["FINDINGS"]

                unnested.append(new_row_inv.copy())

    return unnested


parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest = 'out_loc', required = True,
                    help = 'fully specified location to upload the ndjson file')
args = vars(parser.parse_args())

bucket = f"{os.environ['GCS_PREFIX']}_computronix"

# CX ODATA API URL service root
root = 'https://staff.onestoppgh.pittsburghpa.gov/pghprod/odata/odata/'

# PLI Entity
base = "CASEFILE"

# unnested expansion (only on base table)
unnested_table_1 = "INVESTIGATION"
unnested_table_2 = "VIOLATION"
unnested_table_3 = "CASEFILETYPE"

# fields to select from each table (nt = nested table; unt = unnested table)
fds_base = "EXTERNALFILENUM, STATUSDESCRIPTION, ADDRESSFORMATTEDADDRESS, PARCELPARCELNUMBER"
fds_unt1 = "DATECOMPLETED, OUTCOME, FINDINGS"
fds_unt2 = "CODESECTION, CODESECTIONTITLE, DESCRIPTION, SPECIALINSTRUCTIONS"
fds_unt3 = "NAME"

# build filter which excludes data before implementation of CX system (6/20) and data that have not yet been
# inspected (which are not meaningul at that point. The data will be included when the subsequent inspection occurs)
today = date.today()
record_filter = F"$filter=DATECOMPLETED gt 2020-06-01T00:00:00Z and DATECOMPLETED lt {today}T00:00:00Z "

# build url
odata_url = F"{root}{base}?" \
            F"$select={fds_base}, " \
            "&" \
            F"$expand={unnested_table_1}" \
            F"($select={fds_unt1};{record_filter}), " \
            F"{unnested_table_2}($select={fds_unt2}), " \
            F"{unnested_table_3}($select={fds_unt3})"

# get violations from API
violations = call_odata_api(odata_url)

# names to swap for fields that are not nested in raw data
swaps = [
        fds_base.split(", "),
        ["casefile_number", "status", "address", "parcel_num"]
]
unnested_violations = unnest_violations(violations, swaps)

# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_violations.json
json_to_gcs(args["out_loc"], unnested_violations, bucket)