#!/bin/sh

terraform init
{ bash passcode.sh; } | terraform plan
{ bash passcode.sh; } | terraform apply -auto-approve
