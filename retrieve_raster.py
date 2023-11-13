import subprocess
import os

def retrieve_raster(schema, table, output_file, band = None):

    # Temporary system variable to different PYPROJ instalation
    os.environ["PROJ_LIB"] = "C:/Users/Acer/PycharmProjects/pythonProject2/venv/Lib/site-packages/pyproj/proj_dir/share/proj"

    # Database connection parameters
    database_name = "dbsample"
    username = "postgres"
    password = "ecotenistheway"
    host = "test-data-1-instance-1.cteav6vpg16n.eu-central-1.rds.amazonaws.com"
    port = "5432"

    # Define band parameter 
    if band:
        b = f"-b {band} "
    else:
        b = ""

    # Download command
    command = f'gdal_translate {b}-of GTiff PG:"host={host} port={port} dbname={database_name} user={username} password={password} schema={schema} table={table} mode=2" {output_file}'

    # Run the command
    subprocess.run(command, shell=True)


retrieve_raster("berlin", "cs_2013_berlin", "C:/Users/Acer/PycharmProjects/Germany/berlin_cs_op_2013.tiff")