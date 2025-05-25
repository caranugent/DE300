Readme.md file for the program 'Homework 03.ipynb'

This program was written by Cara Nugent for Prof. Moses Chen's Data Science and Engineering 300 course at Northwestern University in completion of the assignment Homework 03.

***------------------------ HOW TO RUN PROGRAM ---------------------------------------------------------***  

To run this program: 
- clone the repo @ git@github.com:caranugent/DE300.git
- open docker daemon on desktop
- navigate to the folder 'homework_3'
- build the daemon in terminal:
    docker build -t hw03 .
- run in terminal: 
    docker run -v "$(pwd)/notebooks:/home/jovyan" -v "$(pwd)/data:/home/jovyan/data" -p 8888:8888 hw03
- open the ip address from terminal into browser
- in collab, navigate into the 'work' folder 
- open the colb notebook 'Homework 03.ipynb'
- run the cells

***------------------------- EXPECTED OUTPUTS ----------------------------------------------------------***  

When this program is run, there should be the following outputs per cell. 

***Cell 10 Output:*** The calculated tf-idf values for first five documents.

***Cell 15 Output:*** The calculated prediction for the given data with lambda = 0.1.


