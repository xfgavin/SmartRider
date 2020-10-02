#!/usr/bin/env bash
ansible-playbook -i inventory.yml --private-key=$AWS_SSH_KEY -u $AWS_USER -f 10 config_storage.yml
