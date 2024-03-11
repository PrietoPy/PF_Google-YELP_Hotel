import functions_framework
import json
import polars as pl
import googlemaps
from google.cloud import bigquery as bq
from google.cloud import storage
import io

gmaps = googlemaps.Client(key='AIzaSyDXtkYLqMyAdQTlAN2txtsJMxZkrblEn-0')

def proceso(elemento):
    lineas = elemento.split('\n')
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
    return df
# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
    files=name[0]+".parquet"
    print("abriendo archivo")
    client = storage.Client()
    inbucket = client.bucket(bucket)
    file_obj = inbucket.blob(name)
    with file_obj.open() as f:
        data = f.read()
    print("realizando transformaciones")
    df1 = proceso(data)
    print("almacenando resultado")
    outbucket = client.bucket('clean-data-hotel')
    out_obj = outbucket.blob(files)
    # Write the DataFrame to a BytesIO object in-memory
    with io.BytesIO() as stream:
        df1.write_parquet(stream)
        stream.seek(0)  # Move the stream pointer to the beginning
        dataout = stream.getvalue()

    # Upload the BytesIO content to Cloud Storage
    out_obj.upload_from_string(dataout, content_type="application/octet-stream")
    
    print("creando acceso a bq")
    bq_client = bq.Client()
    table_id='hotelwise2024.hwdb1.hoteles'
    file_path = f'gs://clean-data-hotel/{files}' 
    job_config=bq.LoadJobConfig(
              source_format=bq.SourceFormat.PARQUET,
          )
    project_id = 'hotelwise2024'
    print("Ingesta de datos a BQ")
    job = bq_client.load_table_from_uri(
        file_path,
        table_id,
        job_config=job_config,
        project=project_id,
    )
    job.result()  # Wait for the job to complete
    print("datos insertados")