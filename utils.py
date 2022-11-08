import pandas as pd
from google.oauth2 import service_account
import pydata_google_auth
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import gspread
import json

class GoogleSheet:
    def __init__(self):
        self.json_path = "/home/ubuntu/automation/DemandForecastOrdersheets/credfile/dbwisely-v2-01bfe15ef302.json"
        # self.json_path = "./credfile/dbwisely-v2-01bfe15ef302.json"
        self.scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive',
            ]
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(self.json_path, self.scope)
        self.auth = gspread.authorize(self.creds)

    def open_sheet(self, sheet_url, tab_name):
        return get_as_dataframe(self.auth.open_by_url(sheet_url).worksheet(tab_name), evaluate_formulas=True, header = None)


def get_gs_structure():
    with open("/home/ubuntu/automation/DemandForecastOrdersheets/credfile/googlesheets_names_and_ids.json") as f:
    #with open("./credfile/googlesheets_names_and_ids.json") as f:
        names_and_ids = json.load(f)

    gs = GoogleSheet()
    default_gsurl = 'https://docs.google.com/spreadsheets/d/'
    googlesheets_json = {}

    for ssname in names_and_ids:
        spreadsheets = gs.auth.open_by_url(default_gsurl+names_and_ids[ssname])
        worksheets = spreadsheets.worksheets()

        worksheets_dict = {}
        for ws in worksheets:
            worksheets_dict[ws.title] = ws.id
        googlesheets_json[spreadsheets.title] = {'id':spreadsheets.id, 'worksheets':worksheets_dict}

    with open("/home/ubuntu/automation/DemandForecastOrdersheets/credfile/googlesheets_info.json", 'w') as f:
    #with open("./credfile/googlesheets_info.json", 'w') as f:
        json.dump(googlesheets_json, f, ensure_ascii=False, indent=4)
    return googlesheets_json








