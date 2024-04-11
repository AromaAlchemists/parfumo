import os
import logging
from typing import List
import pandas as pd
from utils.constant_util import *

def concat_chart_feature():
    src_path = os.path.join(DOWNLOADS_DIR, f'perfume_detail/{NOW_DATE}/{NOW_DATE}_perfume_detail.csv')
    df = pd.read_csv(src_path)

    df['Type'] = 0
    df['Animal'] = 0
    df['Aquatic'] = 0
    df['Floral'] = 0
    df['Chypre'] = 0
    df['Creamy'] = 0
    df['Earthy'] = 0
    df['Fougère'] = 0
    df['Fresh'] = 0
    df['Fruity'] = 0
    df['Gourmand'] = 0
    df['Green'] = 0
    df['Resinous'] = 0
    df['Woody'] = 0
    df['Leathery'] = 0
    df['Oriental'] = 0
    df['Powdery'] = 0
    df['Smoky'] = 0
    df['Sweet'] = 0
    df['Synthetic'] = 0
    df['Spicy'] = 0
    df['Citrus'] = 0

    df['Audience'] = 0
    df['Youthful'] = 0
    df['Mature'] = 0
    df['Feminine'] = 0
    df['Masculine'] = 0

    df['Season'] = 0
    df['Spring'] = 0
    df['Summer'] = 0
    df['Fall'] = 0
    df['Winter'] = 0

    df['Occasion'] = 0
    df['Evening'] = 0
    df['Business'] = 0
    df['Night Out'] = 0
    df['Leisure'] = 0
    df['Sport'] = 0
    df['Daily'] = 0

    return df