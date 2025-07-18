#!/bin/bash

#
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Print all intermediate results for debugging when profile analysis fails
function debug_profile() {
    echo "label: ${label}"

    # Query the loads table for the current label
    sql_loads="SELECT * FROM information_schema.loads WHERE LABEL = '${label}';"
    echo "SQL (loads): $sql_loads"
    loads_result=$(${mysql_cmd} -e "$sql_loads")
    echo "loads result:"
    echo "$loads_result"

    # Parse the profile id
    sql_profile_id_and_parsed="SELECT PROFILE_ID, regexp_split(PROFILE_ID, ',')[1] AS parsed_id FROM information_schema.loads WHERE LABEL = '${label}';"
    echo "SQL (PROFILE_ID and parsed_id): $sql_profile_id_and_parsed"
    profile_id_info=$(${mysql_cmd} -e "$sql_profile_id_and_parsed" | tail -n +2)
    echo "PROFILE_ID and parsed_id: $profile_id_info"
    profile_id_parsed=$(echo "$profile_id_info" | awk '{print $2}')

    # Query get_query_profile with the parsed id
    sql_profile="SELECT get_query_profile('${profile_id_parsed}') AS result;"
    echo "SQL (get_query_profile): $sql_profile"
    profile_result=$(${mysql_cmd} -e "$sql_profile")
    echo "get_query_profile result:"
    echo "$profile_result"
}

function check_keywords() {
    profile=$1
    keywords=$2
    expect_num=$3

    actual_num=$(echo -e "${profile}" | grep -c "${keywords}")
    if [ "${expect_num}" -eq "${actual_num}" ]; then
        echo "Analyze profile succeeded"
    else
        echo "Analyze profile failed, keywords: ${keywords}, expect_num: ${expect_num}, actual_num: ${actual_num}"
        echo -e "${profile}"
        debug_profile
    fi
}

sql=$(cat << EOF
SELECT get_query_profile(regexp_split(PROFILE_ID, ",")[1]) AS result FROM information_schema.loads WHERE LABEL = '${label}';
EOF
)

output=$(${mysql_cmd} -e "$sql")
check_keywords "$output" "LoadChannel:" "1"
check_keywords "$output" "Index (id=" "3"