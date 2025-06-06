Readme.md file for the programs 'DAG1.py', 'DAG2.py', and 'DAG3.py'

This program was written by Cara Nugent for Prof. Moses Chen's Data Science and Engineering 300 course at Northwestern University in completion of the assignment Homework 03.

***------------------------ HOW TO RUN PROGRAM ---------------------------------------------------------***  

To run this program: 
- navigate to AWS MWAA 
- navigate to the environment 'nugent-MWAA'
- open Airflow UI and activate:
    - 'collect_weather_data'
    - 'daily_weather_dashboard'
- let run for 48 hours
- activate 'train_weather_model'
- the output data will be stored in the s3 bucket 'nugent-bucket'

NOTE: the 3 DAG files can be found @ git@github.com:caranugent/DE300/tree/main/homework_4.git
- these three files can be reuploaded to the s3 bucket 'nugent-bucket' in the DAGS folder

***------------------------- EXPECTED OUTPUTS ----------------------------------------------------------***  

When this program is run, there should be the following outputs saved to the s3 bucket 'nugent-bucket': 

***weather_data/:*** This folder should contain 24 csv files containing extracted weather data

***output_graphs/:*** This folder should contain png files with daily graphs of the 'temperature', 'visibility', and 'relativeHumidity'.  

***predictions/:*** This folder should contain 2 csv files containing predicted temperatures for the next 8 hours from the first 20 hours of data and the first 40 hours of data (one file for each).

