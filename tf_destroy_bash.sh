#!/bin/sh
{ bash passcode.sh; } | terraform destroy -auto-approve
