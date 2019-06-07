## Downloading laboratory boilerplate

- Clone repo an move to downloaded folder
 
```terminal
git clone https://gitlab.com/liesner/bts-rtda-lab-9.git
cd bts-rtda-lab-9
```


- Download the [Stack Overflow 2019 Developer Survey](https://drive.google.com/open?id=1QOmVDpd8hcVYqqUXDXf68UMDWQZP0wQV) (Extract the files directly to ```bts-rtda-lab-9/data``` folder). The data set contain the following files:
    
    - ```survey_results_schema.csv```: contain a description of each one of the columns on ```survey_results_public.csv```.    
    - ```survey_results_public.csv```: contain the answers of each one of the participants on the survey.  
    - ```so_survey_2019.pdf```: contain the full description of the survey.
    
- Build docker images:
```bash
docker-compose build
```

- Start docker container cluster:

```bash
docker-compose up
```

- Open a bash section on spark container:
```
docker exec -it spark bash
```


- Run test and produce package of apark app

```bash
cd /appdata
sbt test
sbt package
```


- check connectivity to  elasticsearch container

```bash
ping elasticsearch
```

- Submit app to spark cluster
```bash
spark-submit --master local[2] --packages org.elasticsearch:elasticsearch-spark-20_2.11:7.1.1 --class Main target/scala-2.11/bts-rtda-lab-9_2.11-1.jar /appdata/data/survey_results_public.csv
```

-- Check index on elasticsearch using kinana sweb server

```bash
http://0.0.0.0:5601
user: elastic
pass: changeme
```


# Complete the boilerplate implementing the following use cases:

- Should created an index on elasticsearch wich contain a view: "Average year writing first line (Age1stCode) of code grouped by sex (Gender) and sorted by avergae descendant"
- Should create an index on elasticsearch wich contain a view: "Average professional coding Experience (YearsCodePro) by developer type (DevType) sorted by average".
- Should created an index on elasticsearch wich contain a view: "Percentage of developers that are Students (Student) sorted by percentage"
- Should created an index on elasticsearch wich contain a view: "Percentage of developers by race and ethnicity (Ethniticity) sorted by percentage (not include NA values on Ethnicity column)"
- Should created an index on elasticsearch wich contain a view: "Percentage of use of social media (SocialMedia) types amon developers sorted by percentage".
    
# Assignment

- Complete the spark boiler plate provided with code to produce indexes on elasticsearch with the following views:
    
    - Age average by Country
    - Percentage of programmers by languaje.  
    - Percentage of programmers by platform.

- Create test code for each functionality developed 
- Create a dashboard on kibana with a graphic for each of the views. 
