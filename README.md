# relational_tracker

## **Problem this repository is meant to solve** 

The Airtable relational base form has volunteers identifying voters they recognize in the district. Each voter is then associated with that volunteer. The second step of that process has volunteers giving Support Score assessments for only the voters they have identified, which is not possible through the standard Airtable form or through a custom view (which is not editable by non-Airtable/non-base users). Volunteers are not allowed to access the full Airtable base itself because it would give them too much access to backend information and because it would be too much of a hassle for casual volunteers to have to create an Airtable account. To solve this issue, this script creates a pipeline between Airtable and Google Sheets, with Google Sheets serving as the sheet where volunteers input Support Score assessments. 

**relational_tracker_pull.py**

The script checks Airtable on a set schedule (i.e. every 10 min, 30 min, 1 hr, etc.) for new volunteers and new relational IDs, and pulls data (Voter Name, Address, Phone, Volunteer, and Support Scores) from Airtable for each volunteer in a list and sets it in their respective Google Sheet. Additional relational IDs are appended to the sheet. Every time a new volunteer sheet is created or new relational IDs are made, the volunteer and field director are emailed a notification with a link to the sheet along with instructions. New volunteer spreadsheets are added to a master spreadsheet tracker that contains all active volunteers, contact info, and links to their spreadsheets for the campaign.

**relational_tracker_push.py (in development)**

The script pushes data from each volunteer spreadsheet in Google Sheets to Votebuilder (VAN), updating only support score survey questions based on VANID (hidden column in the Google sheet).
