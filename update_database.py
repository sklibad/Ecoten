import psycopg2
import os
import geopandas as gpd
import pandas as pd
import warnings
import sys
from pyproj import CRS
from PyQt5.QtWidgets import QApplication, QPushButton, QComboBox, QVBoxLayout, QDialog, QMessageBox
from sqlalchemy import *
from shapely.geometry import MultiPolygon

# Potential pyproj version warning prevention
#os.environ["PROJ_LIB"] = "C:/Users/Acer/PycharmProjects/pythonProject2/venv/Lib/site-packages/pyproj/proj_dir/share/proj"

# Initialize the QApplication
app = QApplication(sys.argv)

# Get the absolute path
script_dir = os.path.dirname(os.path.abspath(__file__))

def popup_dropdown():
    # Popup window with dropdown menu
    popup = QDialog()
    popup.setWindowTitle("Column data type")
    
    # Create a list of options
    options = ["NUMERIC", "INTEGER", "FLOAT", "CHAR(10)", "CHAR(20)", "CHAR(30)", "CHAR(40)", "CHAR(50)", "TEXT", "DATE", "TIME", "BOOLEAN"]
    
    # Create a dropdown menu
    dropdown = QComboBox()
    dropdown.addItems(options)
    
    def on_ok():
        # Close the modal dialog
        popup.accept()
        
    ok_button = QPushButton("OK")
    ok_button.clicked.connect(on_ok)
    
    layout = QVBoxLayout()
    layout.addWidget(dropdown)
    layout.addWidget(ok_button)
    
    popup.setLayout(layout)
    
    # Show the modal dialog
    result = popup.exec_()  

    # Check if OK button was clicked
    if result == QDialog.Accepted:
        return dropdown.currentText()
    else:
        return None

def add_column_confirmation(column_name):
    # Create popup window with "yes" and "no" options 
    msg_box = QMessageBox()
    msg_box.setIcon(QMessageBox.Question)
    msg_box.setWindowTitle("Confirmation")
    msg_box.setText(f"Do you want to add '{column_name}' as a new column?")
    msg_box.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
    
    result = msg_box.exec_()
    
    return result == QMessageBox.Yes

def execute_sql_script_with_string_literal(filename, conn, table_name):
    # Execute SQL script from a file with string literal parameter
    with open(filename, 'r') as file:
        sql_script = file.read()
        cursor = conn.cursor()
        cursor.execute(sql_script, (table_name,))
    return cursor

def execute_sql_script_with_identifier(filename, conn, table_name):
    # Execute SQL script from a file with identifier parameter
    with open(filename, 'r') as file:
        sql_script = file.read()
        cursor = conn.cursor()
        cursor.execute(sql_script % table_name)
    return cursor

def connect_to_database():
    # Establish a connection to the PostgreSQL database
    try:
        conn = psycopg2.connect(
            host="test-data-1-instance-1.cteav6vpg16n.eu-central-1.rds.amazonaws.com",
            port="5432",
            database="dbsample",
            user="postgres",
            password="ecotenistheway"
        )
        """
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="postgres",
            user="postgres",
            password="ecotenistheway"
        )
        print("Database connected successfully")
        """
    except:
        print("Database not connected successfully")

    return conn

def column_check_and_update(conn, table_name, function_path_columns, function_path_p_key, df, file_format):
    # Compare column names of both data sources and decide whether to add new ones to the database table
    
    # Unnecessary columns of input data
    columns_to_delete = []

    # Primary key column name of the database table
    p_key_column = get_primary_key_column_name(conn, table_name, function_path_p_key)

    # Execute the SQL script to get a list of database table column names
    cursor = execute_sql_script_with_string_literal(function_path_columns, conn, table_name)
    database_column_names = cursor.fetchall()
    database_column_names = [row[0] for row in database_column_names]

    # Get input file column names
    file_column_names = df.columns.tolist()

    # If input file doesn't have the primary key column
    if p_key_column not in file_column_names:
        raise ValueError("The input file is missing matching column name with primary key")
    
    # If the input file has a geometry column
    if file_format == "shp":
        # Remove geometry column name from input file column names
        file_column_names.remove("geometry")

    # Browse input file column names
    for column in file_column_names:
        # If column name doesn't match any database table column name
        if column not in database_column_names:
                # If the column consists of null values only
                if df[column].isna().all():
                    # Add the column to unnecessary columns of input data
                    columns_to_delete.append(column)
                    continue

                # Ask user if he wants to add new column to the database table
                result = add_column_confirmation(column)

                # If he wants to add the new column
                if result:
                    # Ask user on the new column data type
                    data_type = popup_dropdown()

                    # If the data type is selected
                    if data_type is not None:
                        # Add new column to the database table
                        cursor = conn.cursor()
                        sql = "ALTER TABLE {} ADD COLUMN {} {};".format(table_name, column, data_type)
                        cursor.execute(sql)
                        conn.commit()

                    # If user cancels the popup window (assuming he no more wants to add new column)
                    else:
                        # Add the column to unnecessary columns of input data
                        columns_to_delete.append(column)
                
                # If user doesn't want to add new column 
                else:
                    # Add the column to unnecessary columns of input data
                    columns_to_delete.append(column)

    # If there is at least on unnecessary column    
    if len(columns_to_delete) > 0:
        # Delete all the columns we don't want to upload to the database
        df.drop(columns = columns_to_delete, inplace = True)

    cursor.close()
    
    return df

def check_crs(conn, table_name, function_path, shp_path):
    # Prevent issues due to not identical coordinate systems of 2 data sources

    # Create GeoDataFrame object
    gdf = gpd.read_file(shp_path)

    # Get input file crs
    shp_crs = gdf.crs

    # Get database table crs
    cursor = execute_sql_script_with_identifier(function_path, conn, table_name)
    database_crs_epsg = cursor.fetchall()[0][0]
    cursor.close()
    database_crs = CRS.from_epsg(database_crs_epsg)

    # If crs of both data sources don't match
    if database_crs != shp_crs:
        # Raise a warning
        warnings.warn("Data sources don't have identical coordinate system!", UserWarning)

        # Transform the input file crs into the database table one
        gdf = gdf.to_crs(database_crs)

    return gdf

def get_file_format(file_path):
    # Get the input file extension
    file_extension = os.path.splitext(file_path)[1]

    # Remove the leading dot (if present) from the extension
    file_extension = file_extension.lstrip(".")

    return file_extension

def get_primary_key_column_name(conn, table_name, function_path):
    # Execute the SQL script to get the primary key database table column name
    cursor = execute_sql_script_with_string_literal(function_path, conn, table_name)

    p_key_column = cursor.fetchall()[0][0]
    
    return p_key_column

def update_database(df, table_name, function_path, conn, file_format):
    # Update database with new rows from the input file

    # Create sqlalchemy engine
    db_url = 'postgresql://postgres:ecotenistheway@test-data-1-instance-1.cteav6vpg16n.eu-central-1.rds.amazonaws.com:5432/dbsample'
    #db_url = 'postgresql://postgres:ecotenistheway@localhost:5432/postgres'
    engine = create_engine(db_url, echo = False)

    # If the input file is shapefile
    if file_format == "shp":
        # Browse all features
        for idx, row in df.iterrows():
                # If current feature geometry type is Polygon
                if row['geometry'].geom_type == 'Polygon':
                    # Change the geometry type to MultiPolygon
                    df.at[idx, 'geometry'] = MultiPolygon([row['geometry']])

        # Execute the SQL script to get the database table column name with stored geometry
        cursor = execute_sql_script_with_string_literal(function_path, conn, table_name)
        geom_col = cursor.fetchall()[0][0]

        # If the database geometry table column name doesn't match a GeoDataFrama column name
        if geom_col != "geometry":
            # Rename and redefine the GeoDataFrame geometry column
            df = df.rename(columns={'geometry': geom_col})
            df = df.set_geometry(geom_col)

        # Use the GeoDataFrame to_postgis method to upload to the database
        df.to_postgis(table_name, engine, if_exists = 'append', index = False)

    #If the input file is csv
    elif file_format == "csv":
        # Use the DataFrame to_sql method to upload to the database
        df.to_sql(table_name, engine, if_exists = 'append', index = False)


# SQL scripts
get_column_names = os.path.join(script_dir, "get_column_names.sql")
get_crs = os.path.join(script_dir, "get_crs.sql")
get_p_key = os.path.join(script_dir, "get_primary_key.sql")
get_geom_col_name = os.path.join(script_dir, "get_geometry_column_name.sql")

# Database table name
table_to_be_updated = 'cities'

# Input file
data_to_be_uploaded = os.path.join(script_dir, "Pisek_dissolved.shp")

# Csv file column delimiter
csv_delimiter = ";"

# Create database connection
conn = connect_to_database()

# Get the input file format/extension
input_file_format = get_file_format(data_to_be_uploaded)

if input_file_format == "shp":
    # Create GeoDataFrame with matching crs
    df = check_crs(conn, table_to_be_updated, get_crs, data_to_be_uploaded)
    df["continent"] = "Europe"

elif input_file_format == "csv":
    # Create DataFrame
    df = pd.read_csv(data_to_be_uploaded, delimiter = csv_delimiter)

else:
    raise ValueError(f"Invalid file format for '{data_to_be_uploaded}'. Expected formats: shp, csv")

# (Geo)DataFrame with all the data that's going to be uploaded into the database
df = column_check_and_update(conn, table_to_be_updated, get_column_names, get_p_key, df, input_file_format)

# Append the (Geo)DataFrame to the database table
update_database(df, table_to_be_updated, get_geom_col_name, conn, input_file_format)

# Make the database changes permanent
conn.commit()

# Close the database connection
conn.close()