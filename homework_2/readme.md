Readme.md file for the program 'Homework 02.ipynb'

This program was written by Cara Nugent for Prof. Moses Chen's Data Science and Engineering 300 course at Northwestern University in completion of the assignment Homework 02.

***------------------------ HOW TO RUN PROGRAM ---------------------------------------------------------***  

To run this program: 
- clone the repo @ git@github.com:caranugent/DE300.git
- open docker daemon on desktop
- build the daemon in terminal:
    docker build -t hw02 .
- navigate to the folder 'Homework 02'
- navigate back to the 'DE300' directory
- run in terminal: 
    docker run -v "$(pwd)/Homework 02/notebooks:/home/jovyan/work" -v "$**PATH TO AWS CREDENTAILS**:/home/jovyan/.aws/credentials" -v "$**PATH TO sf-class2-root.crt**:/home/jovyan/sf-class2-root.crt" -p 8888:8888 hw02
- open the ip address from terminal into browser
- in collab, navigate into the 'work' folder 
- open the colb notebook 'Homework 02.ipynb'
- run the cells

***------------------------- EXPECTED OUTPUTS ----------------------------------------------------------***  

When this program is run, there should be the following outputs per cell. 

***Cell 03 Outputs***

After running cell 3 there should be two outputs:
- The heading of the table (ethnicity, drug_type, total_use)
- A bar chart of Drugs Type Usage by Ethnicity

***Cell 06 Outputs***

After running cell 6 there should be two outputs:
- The heading of the table (age_group, short_title, procedure_count)
- A bar chart of Drugs Type Usage by Ethnicity

***Cell 08 Outputs***

After running cell 8 there should be one output:
- A table displaying the average stay time, range of stau. and standard deviation of the stay

***Cell 09 Outputs***

After running cell 9 there should be two outputs:
- The heading of the table (stay_time_days, ethnicity\, count)
- A box plot of ICU Stays by Ethnicity

***Cell 10 Outputs***

After running cell 10 there should be two outputs:
- The heading of the table (stay_time_days, gender, count)
- A box plot of ICU Stays by Gender

***Cell 17 Outputs***

After running cell 17 there should be one output:
- a list of rows in the cassandra_df (ethnicity, drug_type, count)

***Cell 20 Outputs***

After running cell 20 there should be one output:
- a list of rows in the cassandra_df (age_group, procedure, count)

***Cell 21 Outputs***

After running cell 21 there should be three outputs:
- The mean, standard deviation, and range of the ICU stays

***Cell 22 Outputs***

After running cell 22 there should be two outputs:
- The heading of the table (stay_time_days, ethnicity, total_use)
- A box plot of ICU Stay Duration (Days) by Ethnicity

***Cell 23 Outputs***

After running cell 23 there should be two outputs:
- The heading of the table (stay_time_days, gender, total_use)
- A box plot of ICU Stay Duration (Days) by Gender