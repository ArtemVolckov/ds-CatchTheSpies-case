import pandas as pd
import pdfplumber
import xml.etree.ElementTree as ET

# pd.set_option('display.max_colwidth', None)

###################
#   DATA FRAMES   #
###################

# CSV
if False:
    csv_df = pd.read_csv('data/BoardingData.csv')
    print(csv_df.head())

# JSON
if False:
    json_df = pd.read_json('data/FrequentFlyerForum-Profiles.json')
    print(json_df.head())

# TAB
if False:


# YAML
if False:


# XLSX
if False:


# PDF
if False:
    data = []
    i = 0

    with pdfplumber.open('data/Skyteam_Timetable.pdf') as pdf:
        for page in pdf.pages:
            if i > 2:
                break
            text = page.extract_text()

            if text:
                lines = text.split('\n')

                for line in lines:
                    data.append(line)
            i = i + 1

    pdf_df = pd.DataFrame(data, columns=['Text'])
    print(pdf_df.head())

# XML
if False:
    tree = ET.parse('data/PointzAggregator-AirlinesData.xml')
    root = tree.getroot()

    data = []

    for user in root.findall('user'):
        uid = user.get('uid')
        name = user.find('name')

        first_name = name.get('first') if name is not None else None
        last_name = name.get('last') if name is not None else None
        
        cards = user.find('cards')

        if cards is not None:
            for card in cards.findall('card'):
                number = card.get('number')
                bonus_programm = card.find('bonusprogramm').text if card.find('bonusprogramm') is not None else None
                
                activities = card.find('activities')

                if activities is not None:
                    for activity in activities.findall('activity'):
                        code = activity.find('Code').text if activity.find('Code') is not None else None
                        date = activity.find('Date').text if activity.find('Date') is not None else None
                        departure = activity.find('Departure').text if activity.find('Departure') is not None else None
                        arrival = activity.find('Arrival').text if activity.find('Arrival') is not None else None
                        fare = activity.find('Fare').text if activity.find('Fare') is not None else None

                        data.append ({
                            'uid': uid,
                            'first_name': first_name,
                            'last_name': last_name,
                            'card_number': number,
                            'bonus_programm': bonus_programm,
                            'activity_code': code,
                            'activity_date': date,
                            'departure': departure,
                            'arrival': arrival,
                            'fare': fare
                        })

    xml_df = pd.DataFrame(data)
    print(xml_df.head())
