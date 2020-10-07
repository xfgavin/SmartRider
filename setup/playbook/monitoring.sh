#!/usr/bin/env bash
sed -e "s/AWSUSER/$AWS_USER/g" monitoring.yml > monitoring_tmp.yml
ansible-playbook -i inventory.yml --private-key=$AWS_SSH_KEY -u $AWS_USER -f 10 monitoring_tmp.yml
rm -rf monitoring_tmp.yml
