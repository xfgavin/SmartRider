Since I have to ssh to my spark cluster, I did the following to use airflow container:
  1. docker exec into container,
  2. ssh to spark cluster node, 
  3. accept host key,
  4. copy ~/.ssh/known_hosts out.
  5. modify docker_compose.yml to mount known_hosts back to container in case that known_hosts may be lost because of restart
  6. modify docker_compose.yml to mount your private key for password less ssh inside container.
