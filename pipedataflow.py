import argparse
import json
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToBigQuery
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner
import polars as pl
import googlemaps

gmaps = googlemaps.Client(key='AIzaSyDvGPyow2tycoXCfB8VyKpmFeP1e1U7sqU')

class TransformData(beam.DoFn):
  def process(self, element):
    lineas = element.split('\n')
    names, gmap_id, descrptions, latitudes, longitudes, categories, avg_ratings, num_of_reviews, MISCs, urls = [], [], [], [], [], [], [], [], [], []
    for linea in lineas:
        if linea.strip():  # Ignora líneas en blanco
            objeto_json = json.loads(linea)
            names.append(objeto_json['name'])
            gmap_id.append(objeto_json['gmap_id'])
            descrptions.append(objeto_json['description'])
            latitudes.append(objeto_json['latitude'])
            longitudes.append(objeto_json['longitude'])
            if objeto_json['category']:
                c=0
                for categoria in objeto_json['category']:
                    if 'hotel' in categoria.lower():
                        c+=1
                        categories.append(objeto_json['category'])
                        break
                if c == 0:
                    categories.append(None)
            else:
                categories.append(None)
            avg_ratings.append(objeto_json['avg_rating'])
            num_of_reviews.append(objeto_json['num_of_reviews'])
            servicios = []
            if (objeto_json['MISC'] != 'null') and (objeto_json['MISC'] != 'None') and objeto_json['MISC']:    
                for key,value in objeto_json['MISC'].items():
                    if (value != 'null') and (value != 'None') and value and key not in ['Health & safety', 'Planning']:
                        servicios.extend(value)
            else:
                servicios = None
            if servicios == []:
                servicios = None
            MISCs.append(servicios)
            urls.append(objeto_json['url'])
    data = {
    "name": names,
    "gmap_id": gmap_id,
    "descrption": descrptions,
    "latitude": latitudes,
    "longitude": longitudes,
    "category": categories,
    "avg_rating": avg_ratings,
    "num_of_reviews": num_of_reviews,
    "facilities": MISCs,
    "url": urls
    }
    df = pl.DataFrame(data)
    df = df.filter(~pl.col('category').is_null())
    df = df.unique(subset=['gmap_id'], keep='first') 
    
    counties, cities, states, countries, = [], [], [], []

    for lat, lon in zip(df['latitude'], df['longitude']):
        resultado = gmaps.reverse_geocode((lat, lon))
        county, city, state, country = None, None, None, None

        if resultado:
            for component in resultado[0]['address_components']:
                if 'locality' in component['types'] and not city:
                    city = component['long_name']

                elif 'administrative_area_level_2' in component['types'] and not county:
                    county = component['long_name']

                elif 'administrative_area_level_1' in component['types'] and not state:
                    state = component['long_name']

                elif 'country' in component['types'] and not country:
                    country = component['long_name']
                elif city and county and state and country:
                  break

        counties.append(county)
        cities.append(city)
        states.append(state)
        countries.append(country)

    counties, cities, states, countries = pl.Series(counties), pl.Series(cities), pl.Series(states), pl.Series(countries)
    df = df.with_columns(
    County=counties,
    City=cities,
    State=states,
    Country=countries)
    di = df.to_dict(as_series=False)
    return di

def pipeline():
  # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--bucket', required=True, help='Specify Google Cloud storage bucket')
    parser.add_argument('--stagingLocation', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--tempLocation', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--file', required=True, help='Specify file')

    opts = parser.parse_args()
    project = opts.project
    bucket = opts.bucket
    file=opts.file

    # Setting up the Beam pipeline options
    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('pipego',opts.file.replace(".",""))
    options.view_as(StandardOptions).runner = opts.runner

    # Static input and output
    input = f'gs://{bucket}/{file}'
    output = f'{project}:dbhw.hoteles'

    table_schema = {
        "fields": [
            {
                "name": "name",
                "type": "STRING"
            },
            {
                "name": "gmap_id",
                "type": "STRING"
            },
            {
                "name": "description",
                "type": "STRING"
            },
            {
                "name": "latitude",
                "type": "FLOAT"
            },
            {
                "name": "longitude",
                "type": "FLOAT"
            },
            {
                "name": "category",
                "type": "STRING"
            },
            {
                "name": "avg_rating",
                "type": "FLOAT"
            },
            {
                "name": "num_of_reviews",
                "type": "INTEGER"
            },
            {
                "name": "facilities",
                "type": "STRING"
            },
            {
                "name": "url",
                "type": "STRING"
            },
            {
                "name": "County",
                "type": "STRING"
            },
            {
                "name": "City",
                "type": "STRING"
            },
            {
                "name": "State",
                "type": "STRING"
            },
            {
                "name": "Country",
                "type": "STRING"
            }
        ]
    }
  
    with beam.Pipeline(options=options) as pipeline:
        # Read data from Cloud Storage
        data = pipeline | 'ReadFromText' >> ReadFromText(input)

        # Apply transformations using your DoFn
        transformed_data = data | 'TransformData' >> beam.ParDo(TransformData())

        # Write transformed data to BigQuery
        transformed_data | 'WriteToBigQuery' >> WriteToBigQuery(
            table=output,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )

# Run the pipeline
if __name__ == '__main__':
    pipeline()

