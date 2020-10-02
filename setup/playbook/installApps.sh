#!/usr/bin/env bash
sed -e "s/AWSID/$AWS_ACCESS_KEY_ID/g" -e "s/AWSKEY/$AWS_SECRET_ACCESS_KEY/g" installApps.yml > installApps_tmp.yml
ansible-playbook -i inventory.yml --private-key=$AWS_SSH_KEY -u $AWS_USER -f 10 installApps_tmp.yml
rm -rf installApps_tmp.yml
