import os
import io
import csv
import json

from google.cloud import storage
from gcs_utils import send_alert_email

storage_client = storage.Client()
bucket = storage_client.get_bucket(f"{os.environ['GCS_PREFIX']}_active_directory")


def send_name_survey(name_part):
    blob = bucket.blob(f"self_id/{name_part}_name_comp000000000000.csv")
    content = blob.download_as_string()
    stream = io.StringIO(content.decode(encoding='utf-8'))

    # DictReader used over Pandas Dataframe for fast iteration + minimal memory usage
    name_reader = csv.DictReader(stream, delimiter=',')
    ad_field = f"ad_{name_part[0]}name"
    c_field = f"ceridian_{name_part[0]}name"
    survey_var = f"AD_{name_part[0].capitalize()}NAME_SURVEY"
    if json.loads(os.environ['USE_PROD_RESOURCES'].lower()):
        for row in name_reader:
            send_alert_email([row['ad_email']], [os.environ['EMAIL']],
                             f"Action Needed: Complete Form With Your Preferred {name_part.capitalize()} Name",
                             f"The Data Services Team has detected a difference between how your {name_part} name is "
                             f"listed for your network login compared to Dayforce. The names we have on file "
                             f"are as follows:<br /><br />"
                             f"<pre>    Dayforce: {row[c_field]}<br />    Microsoft: {row[ad_field]}</pre><br />"
                             f"Please fill out the form linked below with the name you would prefer to have used going "
                             f"forward. Please note that you must enter your name exactly as it should appear, "
                             f"meaning the text is case sensitive and should contain no unnecessary spaces.<br /><br />"
                             f'<a href="https://forms.office.com/g/{os.environ[survey_var]}">'
                             f'{name_part.capitalize()} Name Survey</a>',
                             os.environ['HR_EMAIL'])
    else:
        row = next(name_reader)
        send_alert_email([os.environ['EMAIL']], None,
                         f"Action Needed: Complete Form With Your Preferred {name_part.capitalize()} Name",
                         f"The Data Services Team has detected a difference between how your {name_part} name is "
                         f"listed for your network login compared to Dayforce. The names we have on file "
                         f"are as follows:<br /><br />"
                         f"<pre>    Dayforce: {row[c_field]}<br />    Microsoft: {row[ad_field]}</pre><br />"
                         f"Please fill out the form linked below with the name you would prefer to have used going "
                         f"forward. Please note that you must enter your name exactly as it should appear, "
                         f"meaning the text is case sensitive and should contain no unnecessary spaces.<br /><br />"
                         f'<a href="https://forms.office.com/g/{os.environ[survey_var]}">'
                         f'{name_part.capitalize()} Name Survey</a>',
                         os.environ['HR_EMAIL'])


send_name_survey('first')
send_name_survey('last')
