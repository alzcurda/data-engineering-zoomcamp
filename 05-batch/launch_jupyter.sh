docker run -d -p 8888:8888 \
      -p 4040:4040 \
       --name notebook \
       -v "C:\Git\courses\data-engineering-zoomcamp\05-batch":/home/jovyan/work \
       jupyter/all-spark-notebook:spark-3.5.0

docker logs --tail 6 notebook