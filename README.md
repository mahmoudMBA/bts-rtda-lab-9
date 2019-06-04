# Run test 

- Increase lhost memory elastic

```bash
sysctl -w vm.max_map_count=262144
```

- Run elastic container

```bash
docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.1.1
```


