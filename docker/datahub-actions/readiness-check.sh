#!/bin/bash

if ps -Ao stat= | grep -q '^[Zz]'; then
  exit 1
else
  exit 0
fi