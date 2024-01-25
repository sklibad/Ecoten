import boto3, os, pyproj, subprocess
from osgeo import gdal

def access_s3_bucket(access_key_id, secret_access_key):
    region_name = 'eu-central-1'

    # Create an S3 client using the defined credentials
    s3_resource = boto3.resource('s3', aws_access_key_id = access_key_id, aws_secret_access_key = secret_access_key, region_name = region_name)

    bucket = s3_resource.Bucket('urban-resilience-database')
    
    return bucket

def retrieve_wanted_data(list_of_cities):
    files = []
    for obj in bucket.objects.all():
        city = obj.key.split("/")[0]
        if city in list_of_cities:
            files.append(obj.key)
    return files

def download_files(bucket, files):
    for file in files:
        if "NDWI" in file:
            file_prefix =  "ndwi_"
        elif "EVI" in file:
            file_prefix = "evi_"
        elif "LST" in file:
            file_prefix = "lst_"
        else:
            continue

        temp_directory = os.path.dirname(os.path.abspath(__file__))
        file_name = file.split('_')[0].split("/")[0].lower() + "_" + file_prefix + file.split('_')[-1].split('.')[0] + ".tif"
        temp_path = os.path.join(temp_directory, file_name)
        bucket.download_file(file, temp_path)

def upload_rasters():
    folder_path = os.path.dirname(os.path.abspath(__file__))
    raster_files = [f for f in os.listdir(folder_path) if f.endswith(".tif")]
    for r_file in raster_files:

        r_file_split = r_file.split("_")
        schema = r_file_split[0]
        table_name = r_file_split[1]

        full_path = os.path.join(folder_path, r_file)
        crs = pyproj.CRS("WGS84")
        projection = str(crs.to_epsg())
        raster = gdal.Open(full_path)

        gt = raster.GetGeoTransform()
        pixelSizeX = str(abs(gt[1]))
        pixelSizeY = str(abs(gt[5]))

        temp_file_trans = os.path.join(os.path.dirname(os.path.abspath(__file__)), table_name + ".tif")
        no_data_value = raster.GetRasterBand(1).GetNoDataValue()
        if no_data_value == None:
            no_data_value = 'nan'
        warp_options = gdal.WarpOptions(dstSRS='EPSG:4326', resampleAlg=gdal.GRA_NearestNeighbour, srcNodata = no_data_value, dstNodata = 'nan')
        warp = gdal.Warp(temp_file_trans, raster, options = warp_options)
        
        cmds = f'raster2pgsql -s {projection} -I -C -M {temp_file_trans} -F -t {pixelSizeX}x{pixelSizeY} {schema}.{table_name} | psql -h test-data-1-instance-1.cteav6vpg16n.eu-central-1.rds.amazonaws.com -d third_module -U postgres -p 5432'
        subprocess.call(cmds, shell=True)
        raster = None
        warp = None
        os.remove(full_path)
        os.remove(temp_file_trans)

os.environ["PGPASSWORD"] = "ecotenistheway"
bucket = access_s3_bucket('your-access-key-id', 'your-secret-access-key')
files = retrieve_wanted_data(["Prague"])
download_files(bucket, files)
upload_rasters()
del os.environ["PGPASSWORD"]