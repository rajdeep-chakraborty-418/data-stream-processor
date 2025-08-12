#!/bin/bash

package_name=$(grep "name" setup.py | cut -d '=' -f 2 | cut -d ',' -f 1 | sed "s/'//g" | sed 's/-/_/g')
version=$(grep "version" setup.py | cut -d '=' -f 2 | cut -d ',' -f 1 | sed "s/'//g")
out_wheel=${package_name}-${version}-py3-none-any.whl
echo "${out_wheel}"
