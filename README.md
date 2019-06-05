# Run test 

- Increase lhost memory elastic

```bash
sysctl -w vm.max_map_count=262144
```

- Run elastic container

```bash
docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.1.1
```

- To submit app to spark cluster

```bash
spark-submit --master local[2] --packages org.elasticsearch:elasticsearch-spark-20_2.11:7.1.1 --class Main target/scala-2.11/bts-rtda-lab-9_2.11-1.jar /appdata/data/survey_results_public.csv
```

- Connect to kibana server
```
http://0.0.0.0:5601

user: elastic
pass: changeme
```


# Te application should satisfy the following use case.

    - Should create an index on elesticsearch wich contain e view: "Years of Professional Coding Experience by Developer Type".   
    - 