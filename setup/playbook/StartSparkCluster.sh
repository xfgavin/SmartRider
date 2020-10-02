#!/usr/bin/env bash
ansible-playbook -v -i inventory.yml --private-key=$AWS_SSH_KEY -u $AWS_USER -f 10 StartSparkCluster.yml
