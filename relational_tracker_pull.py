from parsons import Airtable
from parsons import GoogleSheets
import os
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe 
from gspread_formatting import *
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
from apiclient.discovery import build
import numpy as np
import time

while True:

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)
    print('GS creds authorized')

    print('Checking for new Volunteers...')
    vt = Airtable(base_key = '###', table_name = '###', api_key = '###')
    print('AT creds authorized') 
    newvol_formula = '{Connections}!=""'
    vf = pd.read_csv('volunteer_list.csv')
    old_vol_ids = vf['id'].tolist()
    new_vol_list = vt.get_records(fields=['full', 'email', 'phone_number'], formula=newvol_formula, sort='full')
    vf2 = new_vol_list.to_dataframe()
    new_vol_ids = vf2['id'].tolist()
    vf2['email'].fillna('surfersocialist808@gmail.com', inplace=True)

    c = set(old_vol_ids)
    d = set(new_vol_ids)

    if c==d:
        print('The Volunteer lists are equal. No need to create new Volunteer sheets.')
    else:
        print('The Volunteer lists are not equal. Creating new Volunteer sheets.')
        diff1 = list(d-c)
        print(diff1)
        
        for person in diff1:

            vol_counter = 0

            create_vol = vf2.loc[vf2['id'] == person, 'full'].iloc[0]
            create_email = vf2.loc[vf2['id'] == person, 'email'].iloc[0]
            create_phone = vf2.loc[vf2['id'] == person, 'phone_number'].iloc[0]

            ns_formula = 'FIND("%s",{Relationships to Volunteers})' % create_vol

            vot = Airtable(base_key = '###', table_name = '###', api_key = '###')

            new_sheet = vot.get_records(fields=['VANID', 'Full Name',  'Address', 'Preferred Phone', 'Support Scores'], formula=ns_formula, sort='Full Name')

            ns = new_sheet.to_dataframe()
            ns = ns.drop(columns=['createdTime'], axis=1)
            ns = ns[['id', 'VANID', 'Full Name', 'Address', 'Preferred Phone', 'Support Scores']]
            ns['Support Scores'] = ns['Support Scores'].astype(str)
            ns['Phone'] = ns['Preferred Phone'].astype(str)
            ns['VANID'] = ns['VANID'].astype(str).replace('\.0', '', regex=True) 
            ns = ns[['id', 'VANID', 'Full Name', 'Address', 'Phone', 'Support Scores']]

            print('Airtable df created')
            print(ns.head())

            message = 'Aloha! Thank you for finding your friends in the district for Kim Coco. All you need to do now is click on this spreadsheet that has everyone you have identified, make sure you have conversations with them about Kim Coco, and mark in the drop down Support Score column how they feel about voting for Kim Coco. That is it! There is nothing more to it. Bookmark this spreadsheet so you can come back to it whenever you have found new friends in the district, and the spreadsheet will be automatically updated. The campaign will take care of the rest of the data from there!'

            ch = client.create(create_vol + ' - ' + 'People You Know in District 26')
            ch.share(create_email, perm_type='user', role='writer', notify=True, email_message=message)
            ch.share('###', perm_type='user', role='writer', notify=True, email_message=message)
            ch.share('###', perm_type='user', role='writer', notify=True, email_message=message)
            ch.share('###', perm_type='user', role='writer', notify=True, email_message=message)
            print('Spreadsheet shared with emails.')
            create_spreadsheet_id = ch.id
            print(ch.id)
            spreadsheet_url = "https://docs.google.com/spreadsheets/d/%s" % ch.id
            print(spreadsheet_url)
            worksheet = ch.get_worksheet(0)
            set_with_dataframe(worksheet, ns)
            worksheet.update_acell('H1', 'Total People')
            worksheet.update_acell('H2', '=iferror(counta(unique(C:C)))-1')
            worksheet.update_acell('I1', 'Total Assessed')
            worksheet.update_acell('I2', '=COUNTIF(F:F,"1 - Strong Support") + COUNTIF(F:F,"2 - Lean Support") + COUNTIF(F:F,"3 - Undecided") + COUNTIF(F:F,"4 - Lean Opposed") + COUNTIF(F:F,"5 - Strong Opposed")')
            worksheet.update_acell('G1', '')
            worksheet.update_acell('G2', '')
            worksheet.update_acell('H4', '=HYPERLINK("###", "How to talk to your friends about Kim Coco!")')
            fmt = cellFormat(
            textFormat=textFormat(bold=True, foregroundColor=color(1,0,0), fontSize=14,)
            )
            format_cell_range(worksheet, 'A1:I1', fmt)

            fmt2 = cellFormat(horizontalAlignment='RIGHT')
            format_cell_range(worksheet, 'F2:F', fmt2)

            id_list = ns['id'].tolist()
            id_list_length = len(id_list) + 1
            print(id_list_length)

            validation_rule = DataValidationRule(
            BooleanCondition('ONE_OF_LIST', ['1 - Strong Support', '2 - Lean Support', '3 - Undecided', '4 - Lean Opposed', '5 - Strong Opposed', 'None']),
            showCustomUi=True)

            cell_string = "F2:F{}".format(id_list_length)

            set_data_validation_for_cell_range(worksheet, cell_string, validation_rule)

            sheetName = "Sheet1"
            sheetId = ch.worksheet(sheetName)._properties['sheetId']
            
            body = {
                "requests": [
                    {
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": sheetId,
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex": 9
                            },
                            "properties": {
                                "pixelSize": 170
                            },
                            "fields": "pixelSize"
                        }
                    }
                ]
            }
            res = ch.batch_update(body)

            http = creds.authorize(httplib2.Http())
            service = build('sheets', 'v4', http=http)
            requests = []

            requests.append({
            'updateDimensionProperties': {
                "range": {
                "dimension": 'COLUMNS',
                "startIndex": 0,
                "endIndex": 2,
                },
                "properties": {
                "hiddenByUser": True,
                },
                "fields": 'hiddenByUser',
            }})

            body = {'requests': requests}
            response = service.spreadsheets().batchUpdate(
            spreadsheetId=create_spreadsheet_id,
            body=body
            ).execute()

            print(create_vol + ' spreadsheet created at spreadsheet ID - ' + create_spreadsheet_id)

            master_list_id = '###'
            mh = client.open_by_key(master_list_id)
            master_sheet = mh.get_worksheet(0)
            master_sheet.append_row(values=[create_vol, create_phone, create_email, create_spreadsheet_id, spreadsheet_url])

            if (vol_counter == 1):
                print('Resting 100 seconds to avoid rate limit...')
                time.sleep(100)
                vol_counter = 0
            
            vol_counter += 1

            vf2.to_csv('volunteer_list.csv')

            vol_df = pd.read_csv('volunteer_spreadsheet_list.csv')
            vol_df = vol_df[['Name', 'Spreadsheet ID']]
            to_append = [create_vol, create_spreadsheet_id]
            df_length = len(vol_df)
            print(df_length)
            print(to_append)
            print(vol_df.head())
            vol_df.loc[df_length] = to_append
            vol_df.to_csv('volunteer_spreadsheet_list.csv')

            print('Pausing for 10 seconds before checking Connections...')
            time.sleep(10)

    print('Checking for new Connections...')

    at = Airtable(base_key = '###', table_name = '###', api_key = '###')
    print('AT creds authorized')

    df = pd.read_csv('at_conn_ids.csv')
    old_conn_ids = df['id'].tolist()
    new_at_conn_ids = at.get_records(fields=['Volunteer Name'], sort='Volunteer Name')
    df2 = new_at_conn_ids.to_dataframe()
    new_conn_ids = df2['id'].tolist()
    df3 = pd.read_csv('volunteer_spreadsheet_list.csv')

    a = set(old_conn_ids)
    b = set(new_conn_ids)

    if a==b:
        print('The Connections lists are equal. No need to update sheets.')
    else:
        print('The Connections lists are not equal. Updating sheets.')
        diff = list(b-a)
        print(diff)
        for animal in diff:

            rest_counter = 0

            at1 = Airtable(base_key = '###', table_name = '###', api_key = '###')
            print('AT creds authorized') 
            
            new_vol = df2.loc[df2['id'] == animal, 'Volunteer Name'].iloc[0]
            print(new_vol)

            print(new_vol + ' is loafing on his hop and flop')
            new_spreadsheet_id = df3.loc[df3['Name'] == new_vol, 'Spreadsheet ID'].iloc[0]
            print(new_spreadsheet_id)

            volunteers_formula = 'FIND("%s",{Relationships to Volunteers})' % new_vol

            vol = at1.get_records(fields=['VANID', 'Full Name',  'Address', 'Preferred Phone', 'Support Scores'], formula=volunteers_formula, sort='Full Name')

            df4 = vol.to_dataframe()
            df4 = df4.drop(columns=['createdTime'], axis=1)
            df4 = df4[['id', 'VANID', 'Full Name', 'Address', 'Preferred Phone', 'Support Scores']]
            df4['Support Scores'] = df4['Support Scores'].astype(str)
            df4['Phone'] = df4['Preferred Phone'].astype(str)
            df4['VANID'] = df4['VANID'].astype(str).replace('\.0', '', regex=True) 
            df4 = df4[['id', 'VANID', 'Full Name', 'Address', 'Phone', 'Support Scores']]

            print('Airtable df created')
            print(df4.head())

            sh = client.open_by_key(new_spreadsheet_id)
            worksheet = sh.get_worksheet(0)
            df5 = get_as_dataframe(worksheet)
            df5 = df5[['id', 'VANID', 'Full Name', 'Address', 'Phone', 'Support Scores']]   
            df5['VANID'] = df5['VANID'].astype(str).replace('\.0', '', regex=True)    
            print(df5.head())
            df6 = pd.concat([df5, df4])
            print(df6.head())
            df6 = df6.drop_duplicates(subset='VANID', keep="first")
            print(df6.head())
            df6 = df6.dropna()
            print(df6.head())

            set_with_dataframe(worksheet, df6)
            worksheet.update_acell('H1', 'Total People')
            worksheet.update_acell('H2', '=iferror(counta(unique(C:C)))-1')
            worksheet.update_acell('I1', 'Total Assessed')
            worksheet.update_acell('I2', '=COUNTIF(F:F,"1 - Strong Support") + COUNTIF(F:F,"2 - Lean Support") + COUNTIF(F:F,"3 - Undecided") + COUNTIF(F:F,"4 - Lean Opposed") + COUNTIF(F:F,"5 - Strong Opposed")')
            worksheet.update_acell('G1', '')
            worksheet.update_acell('G2', '')
            worksheet.update_acell('H4', '=HYPERLINK("###", "How to talk to your friends about Kim Coco!")')
            fmt = cellFormat(
            textFormat=textFormat(bold=True, foregroundColor=color(1,0,0), fontSize=14,)
            )
            format_cell_range(worksheet, 'A1:I1', fmt)

            fmt2 = cellFormat(horizontalAlignment='RIGHT')
            format_cell_range(worksheet, 'F2:F', fmt2)

            id_list = df6['id'].tolist()
            id_list_length = len(id_list) + 1
            print(id_list_length)

            validation_rule = DataValidationRule(
            BooleanCondition('ONE_OF_LIST', ['1 - Strong Support', '2 - Lean Support', '3 - Undecided', '4 - Lean Opposed', '5 - Strong Opposed', 'None']),
            showCustomUi=True)

            cell_string = "F2:F{}".format(id_list_length)

            set_data_validation_for_cell_range(worksheet, cell_string, validation_rule)

            sheetName = "Sheet1"
            sheetId = sh.worksheet(sheetName)._properties['sheetId']
            
            body = {
                "requests": [
                    {
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": sheetId,
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex": 9
                            },
                            "properties": {
                                "pixelSize": 170
                            },
                            "fields": "pixelSize"
                        }
                    }
                ]
            }
            res = sh.batch_update(body)

            http = creds.authorize(httplib2.Http())
            service = build('sheets', 'v4', http=http)
            requests = []

            requests.append({
            'updateDimensionProperties': {
                "range": {
                "dimension": 'COLUMNS',
                "startIndex": 0,
                "endIndex": 2,
                },
                "properties": {
                "hiddenByUser": True,
                },
                "fields": 'hiddenByUser',
            }})

            body = {'requests': requests}
            response = service.spreadsheets().batchUpdate(
            spreadsheetId=new_spreadsheet_id,
            body=body
            ).execute()

            if (rest_counter == 3):
                print('Resting 100 seconds to avoid rate limit...')
                time.sleep(100)
                rest_counter = 0
            
            rest_counter += 1

        df2.to_csv('at_conn_ids.csv')

    print('Sleeping for 1 hour...')
    time.sleep(600)
