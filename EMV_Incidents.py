import urllib.request, json 
import pandas as pd
from copy import deepcopy
from arcgis.gis import GIS
from arcgis import geometry

# ToDo
# Remove need for Pandas (easier to deploy on Lambda)
# Better Error handling
# Get it to send alerts via AWS when exceptions
# Enrich incidents with other info
            
# Globals
inc_url = 'https://data.emergency.vic.gov.au/Show?pageId=getIncidentJSON'
gis = GIS("https://esriau.maps.arcgis.com/","abc","!")
fl_item = gis.content.get('asdasdasdasdasd')
inc_layer = fl_item.layers[0]

def main():
    # Go fetch EMV Incidents
    print('Getting Incidents into Pandas DF...')
    inc = get_incidents()
    # Create list of features to add into featurelayer
    print('Projecting Geoms and building features into a list...')
    feats = build_feats(inc)
    # Delete all incidents from FL
    print('Deleting Incidents from Feature Layer...')
    del_inc()    
    # Add pre-built feats into the Incidents FeatureLayer
    print('Inserting Incidents into Feature Layer...')
    add_feats(feats)
    print('Finished')


def get_incidents():
    # Get Incident JSON into a dict
    try:
        with urllib.request.urlopen(inc_url) as url:
            data = json.loads(url.read().decode())
    except:
        print("Issue getting Incident JSON")
        exit
        
    df = pd.DataFrame.from_dict(data['results'])

    # Fix up date fields
    df['lastUpdateDateTime'] = pd.to_datetime(df['lastUpdateDateTime'])
    df['originDateTime'] = pd.to_datetime(df['originDateTime'])

    return df
    
def build_feats(df):
    # get a template feature object
    inc_fset = inc_layer.query()
    template_feature = deepcopy(inc_fset.features[0])
    features_to_be_added = []
    
    for row in df.iterrows():
        new_feature = deepcopy(template_feature)
        # print("Creating " + row[1]['name'])
    
        #get geometries in the destination coordinate system
        input_geometry = {'y':float(row[1]['latitude']),
                           'x':float(row[1]['longitude'])}
        output_geometry = geometry.project(geometries = [input_geometry],
                                           in_sr = 4326, 
                                           out_sr = inc_fset.spatial_reference['latestWkid'],
                                           gis = gis)
        
        # assign the new incident values
        new_feature.geometry = output_geometry[0]
        new_feature.attributes['incidentNo'] = row[1]['incidentNo']
        new_feature.attributes['lastUpdateDateTime'] = row[1]['lastUpdatedDt']
        new_feature.attributes['originDateTime'] = row[1]['originDateTime']
        new_feature.attributes['incidentType'] = row[1]['incidentType']
        new_feature.attributes['incidentLocation'] = row[1]['incidentLocation']
        new_feature.attributes['incidentStatus'] = row[1]['incidentStatus']
        new_feature.attributes['incidentSize'] = row[1]['incidentSize']
        new_feature.attributes['name'] = row[1]['name']
        new_feature.attributes['territory'] = row[1]['territory']
        new_feature.attributes['resourceCount'] = row[1]['resourceCount']
        new_feature.attributes['eventCode'] = row[1]['eventCode']
        new_feature.attributes['fireDistrict'] = row[1]['fireDistrict']
        new_feature.attributes['municipality'] = row[1]['municipality']
        new_feature.attributes['category1'] = row[1]['category1']
        new_feature.attributes['category2'] = row[1]['fireDistrict']
        new_feature.attributes['fireDistrict'] = row[1]['category2']
        new_feature.attributes['agency'] = row[1]['agency']
        new_feature.attributes['originStatus'] = row[1]['originStatus']
        new_feature.attributes['type'] = row[1]['type']
        #new_feature.attributes['lastUpdatedDt'] = int(row[1]['lastUpdatedDt'])
        new_feature.attributes['lastUpdatedDtStr'] = row[1]['lastUpdatedDtStr']
        new_feature.attributes['originDateTimeStr'] = row[1]['originDateTimeStr']
        
        features_to_be_added.append(new_feature)
    
    return features_to_be_added
    

def del_inc():
    inc_layer.delete_features(where="objectid > 1")  # need to keep one dummy feature (filter out in view)
    # inc_layer.manager.truncate() # does not work if sync enabled

def add_feats(feats):
    inc_layer.edit_features(adds = feats)



# entry point
if __name__ == "__main__":
    main()
