// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.statistic;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MultiColumnStatsMeta implements Writable {
    @SerializedName("dbId")
    private long dbId;

    @SerializedName("tableId")
    private long tableId;

    @SerializedName("columnIds")
    private Set<Integer> columnIds;

    @SerializedName("analyzeType")
    private StatsConstants.AnalyzeType analyzeType;

    @SerializedName("statisticsTypes")
    private List<StatsConstants.StatisticsType> statisticsTypes;

    @SerializedName("updateTime")
    private LocalDateTime updateTime;

    @SerializedName("properties")
    private Map<String, String> properties;

    public MultiColumnStatsMeta(long dbId, long tableId, Set<Integer> columnIds,
                                StatsConstants.AnalyzeType analyzeType, List<StatsConstants.StatisticsType> statisticsTypes,
                                LocalDateTime updateTime, Map<String, String> properties) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.columnIds = columnIds;
        this.analyzeType = analyzeType;
        this.statisticsTypes = statisticsTypes;
        this.updateTime = updateTime;
        this.properties = properties;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public Set<Integer> getColumnIds() {
        return columnIds;
    }

    public StatsConstants.AnalyzeType getAnalyzeType() {
        return analyzeType;
    }

    public List<StatsConstants.StatisticsType> getStatsTypes() {
        return statisticsTypes;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
