""" Download all TWILIO customer reports and load into GCS
    Coded by Jesse Wood July 2020.

    This script will download all TWILIO customer reports and load the resulting
    data into Google Cloud. This is accomplished with a single function call
    that subsequetly relies on several helper functions to perform all
    processing. Any set of time series reports can be processed, provided they
    ONLY contain summary data (i.e. no calculations need to be performed on the
    data such as summing or averaging). The downloaded report MUST contain a
    date (MUST BE FIRST COLUMN of reprort) that each summary statistic is
    based on. To add additional reports/customers simply specify the report
    name in REPORTS (list of strings constant), indicate if the measure
    requires a conversion to minutes from seconds (TIME_CONV; a boolean
    list), add the workspace ID to the .env file and update the information
    in WORKSPACE (dictionary constant). Finally, add the function call at the
    bottom of the script with all the  necessary arguments.

    This script is easily expandable to process multiple reports from a
    single customer. All that needs to be added are lines that aggregate the
    information from each report into a single dataframe or output the
    reports in seperately.
"""

##
# imports
import requests
import json
import time
import os
import argparse

import pandas as pd
from gcs_utils import storage_client, json_to_gcs

##
# initialize the parser and gcs bucket
parser = argparse.ArgumentParser()
parser.add_argument('-e', '--execution_date', dest = 'execution_date',
                    required = True,
                    help = 'DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())

bucket = '{}_twilio'.format(os.environ['GCS_PREFIX'])

##
# declare constants for API calls
WORKSPACE = {"service_desk": os.environ["TWILIO_SD_WORKSPACE"],
             "311": os.environ["TWILIO_311_WORKSPACE"]}

# name of report to process
REPORTS = ["summary_report"]

# boolean indicating if metric requires conversion from seconds to minutes
TIME_CONV = [False, False, True, True, True]

# URLs for super secure token (SST) an temporay token (TT)
URLS = {"sst": "https://analytics.ytica.com/gdc/account/login",
        "tt": "https://analytics.ytica.com/gdc/account/token"}

# URLs for report (rep) which returns URI for download, and the downlod (DL)
# itselt
URL_REP_BASE = "https://analytics.ytica.com/gdc/app/projects/{}/execute/raw"
URL_DL_BASE = "https://analytics.ytica.com/{}"

DL_ATTEMPTS_PARAMS = {"max_attempts": 5, "time_sleep": 30}

##
# create class to contain the error message used in download_report function
class TwilioReportError(Exception):
    def __init__(self, message):
        self.message = message


##
# define function to download a report from the Twilio API
def download_report(attempt_info, header, url_in):
    """download the API report

    Request Twilio report download from the API. If the data were able to be
    downloaded (status code 200) then extract the byte string literal content
    of the download to a list. This list will be parsed in a subsequent loop.
    The API cannot always send the download right away, meaning the machine
    must wait a predetermined amount of time and try again. Each time the
    download fails, the api_fail flag is set to true. After the specified
    number of download attempts, if the flag is true, then an error is raised.
    """

    for attempts in range(attempt_info["max_attempts"]):
        api_fail = False
        rsp = requests.get(url_in, headers = header)

        # if download successful -> extract content and exit loop
        if rsp.status_code == 200:
            raw_list = rsp.content.decode('utf-8').split('\r\n')
            break

        # else the status code indicates the download is not ready
        else:
            api_fail = True
            time.sleep(attempt_info["time_sleep"])

    # if data could not be downloaded from api after max_attemps
    if api_fail:
        error = TwilioReportError("Twilio report download failed")
        raise TwilioReportError(error.message)

    return raw_list


##
# define function that manages the download
def prep_report_download(wkspc_id, obj, url_rep, hd_uri, attempts, hd_dl,
                         url_dl):
    """Prepare the API request and download the report

        When this function is called by passing in the necessary information
        to populate the URI request payload, the URI (byte string literal) is
        returned and extracted. The report is then downloaded from the API
        (via calling embeded function).
        """

    # define and receive the API request for the download URI
    payload_uri = json.dumps({"report_req": {"report": "/gdc/md/{}/obj/{}".
                             format(wkspc_id, obj)}})

    response_uri = requests.post(url_rep, headers = hd_uri, data = payload_uri)

    # define the API request for the report download. Download is returned as a
    # list from function call
    curr_uri = response_uri.json()['uri']
    url_rep_dl = url_dl.format(curr_uri)
    raw_dl_list = download_report(attempt_info = attempts,
                                  header = hd_dl,
                                  url_in = url_rep_dl)

    # drop the empty row that is at the end of the downloaded list
    raw_dl_list = raw_dl_list[:-1]

    return raw_dl_list


##
# define function to format the report into a dataframe
def format_report_download(raw_report, conv_time):
    """ Clean, transform, and format the raw data from the API download

    The report that is downloaded from the API is returned in a byte string
    literal that is converted into a list by the "download_report" function,
    which is called inside the "prep_report_download" function. Data must
    EXTRACTED from these lists, TRANSFORMED into numeric data types (temporal
    variables are also converted from raw seconds to mintues), and placed
    into a Pandas DataFrame.
    """

    # pop off the column headers; initialize an empty DF (data) with the columns
    cols = raw_report.pop(0).replace('"', "").replace(" ", "_"). \
        lower().split(",")
    data = pd.DataFrame(columns = cols)

    # for each remaining row of the raw list ->
    for r in raw_report:
        # split comma sep strings into a list
        curr_vals = r.replace('"', "").replace(",,", ",0,").replace(
            "/", "-").split(",")

        # pop off the first val (serves as an index)
        new_val = [curr_vals.pop(0)]

        # declare counter that indexes the ith conv_time value
        i = 0

        # vals are popped off and converted to ints ->
        while len(curr_vals) != 0:
            # select the first val in the list and convert to an int
            temp_val = curr_vals.pop(0)
            temp_val = int(temp_val.split('.')[0])

            # if temp_val is raw seconds (indicated by TIME_CONVERSION flag) ->
            # convert to minutes
            if conv_time[i]:
                temp_val = round(temp_val / 60, 2)

            else:
                temp_val = int(temp_val)

            # new_val (list) is reset at top of r loop (reset for each
            # row of the raw download list); add temp_val as list
            new_val = new_val + [temp_val]

            # increment counter
            i += 1

        # while loop concludes when curr_vals (current row of the raw
        # download list) is empty; when while loop is done, put new_val in DF
        # and append to the growing data DF
        data_to_add = pd.DataFrame([new_val], columns = cols)
        data = data.append(data_to_add)

    return data


##
# define function that returns the complete formatted report (NDL JSON)

def get_report(report_name, time_conversion, dl_attempts, url, url_dl_base,
               workspace_id, targ_obj_id, url_report_base, workspace_name):
    """ Return the finalized report
    This function calls all helper functions to retrieve, format,
    and finalize the report Twilio dowloaded report from the API. All
    necessary steps for creating the final report are contained within this
    function."""

    # initialize object IDs with unique identifier for each report
    object_id = {}
    for r in range(len(report_name)):
        object_id.update({report_name[r]: targ_obj_id[r]})

    ##
    # define and receive API request for super secure token (sst)
    header_sst = {"Accept": "application/json",
                  "Content-Type": "application/json"}

    payload_sst = {"postUserLogin": {"login": os.environ['TWILIO_EMAIL'],
                                     "password": os.environ['TWILIO_API_PW'],
                                     "remember": 0,
                                     "verify_level": 2}}

    response_sst = requests.post(url["sst"], headers = header_sst,
                                 data = json.dumps(payload_sst))

    token = {"sst": response_sst.json()['userLogin']['token']}

    ##
    # define and receive API request for temporary token (tt)
    header_tt = {"Accept": "application/json",
                 "Content-Type": "application/json",
                 "X-GDC-AuthSST": token["sst"]}

    response_tt = requests.get(url["tt"], headers = header_tt)
    token.update({"tt": response_tt.json()['userToken']['token']})

    ##
    # define headers for API request for URI and the report download
    header_uri = {"Accept": "application/json",
                  "Content-Type": "application/json",
                  "Cookie": "GDCAuthTT={}".format(token["tt"])}

    header_dl = {"Cookie": "GDCAuthTT={}".format(token["tt"])}

    ##
    # define the API request for the report download, execute the download,
    # and parse the raw download (a byte string literal) into a list (returns
    # the unformatted (raw) report

    # define the report URL
    url_report = url_report_base.format(workspace_id)

    raw_summary_report = prep_report_download(wkspc_id = workspace_id,
                                              obj = object_id["summary_report"],
                                              url_rep = url_report,
                                              hd_uri = header_uri,
                                              attempts = dl_attempts,
                                              hd_dl = header_dl,
                                              url_dl = url_dl_base)

    ##
    # format the raw summary report (list) into a DF
    # function call to format report
    formatted_summary_report = format_report_download(raw_report =
                                                      raw_summary_report.copy(),
                                                      conv_time =
                                                      time_conversion.copy())

    # set date as the index and alter the column names
    formatted_summary_report.set_index('date', inplace = True)
    formatted_summary_report.columns = ["handled_conversations",
                                        "voicemails", "median_talk_time",
                                        "average_talk_time", "total_talk_time"]

    ##
    # write the summary report to a new line delim JSON
    formatted_report = formatted_summary_report.to_dict(orient = "records")


    ##
    # upload formatted report to Google Cloud
    json_to_gcs('{}/{}/{}/{}_{}.json'.format(workspace_name,
                                             args['execution_date'].split(
                                                 '-')[0],
                                             args['execution_date'].split(
                                                 '-')[1],
                                             args['execution_date'],
                                             workspace_name),
                formatted_report,
                bucket)

    return formatted_report


##
# get fully formatted service desk report
report_serv_desk = get_report(report_name = REPORTS,
                              time_conversion = TIME_CONV,
                              dl_attempts = DL_ATTEMPTS_PARAMS,
                              url = URLS,
                              url_dl_base = URL_DL_BASE,
                              workspace_id = WORKSPACE["service_desk"],
                              targ_obj_id = ["119775"],
                              url_report_base = URL_REP_BASE,
                              workspace_name = "service_desk")


##
# get fully formatted 311 report
report_311 = get_report(report_name = REPORTS,
                        time_conversion = TIME_CONV,
                        dl_attempts = DL_ATTEMPTS_PARAMS,
                        url = URLS,
                        url_dl_base = URL_DL_BASE,
                        workspace_id = WORKSPACE["311"],
                        targ_obj_id = ["233466"],
                        url_report_base = URL_REP_BASE,
                        workspace_name = "311")

##
#
