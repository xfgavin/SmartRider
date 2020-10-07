#!/usr/bin/env bash
sed -e "s/AWSUSER/$AWS_USER/g" config_storage.yml > config_storage_tmp.yml
ansible-playbook -i inventory.yml --private-key=$AWS_SSH_KEY -u $AWS_USER -f 10 config_storage_tmp.yml
rm -rf config_storage_tmp.yml
