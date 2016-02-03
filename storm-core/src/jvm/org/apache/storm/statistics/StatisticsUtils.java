/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.statistics;

import org.apache.storm.Config;
import org.apache.storm.statistics.reporters.JmxPreparableReporter;
import org.apache.storm.statistics.reporters.PreparableReporter;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StatisticsUtils {
    private final static Logger LOG = LoggerFactory.getLogger(StatisticsUtils.class);

    public static List<PreparableReporter> getPreparableReporters(Map stormConf) {
        PreparableReporter reporter = new JmxPreparableReporter();
        List<String> clazzes = (List<String>) stormConf.get(Config.STORM_STATISTICS_PREPARABLE_REPORTER_PLUGINS);
        List<PreparableReporter> reporterList = new ArrayList<>();

        if (clazzes != null) {
            for(String clazz: clazzes ) {
                reporterList.add(getPreparableReporter(clazz));
            }
        }
        if(reporterList.isEmpty()) {
            reporterList.add(new JmxPreparableReporter());
        }
        return reporterList;
    }

    private static PreparableReporter getPreparableReporter(String clazz) {
        PreparableReporter reporter = null;
        LOG.info("Using statistics reporter plugin:" + clazz);
        if(clazz != null) {
            reporter = (PreparableReporter) Utils.newInstance(clazz);
        }
        return reporter;
    }
}