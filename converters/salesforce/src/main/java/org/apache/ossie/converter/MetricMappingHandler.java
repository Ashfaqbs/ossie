/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ossie.converter;

import static org.apache.ossie.converter.ConverterConstants.*;
import static org.apache.ossie.util.DataStructureUtils.*;

import org.apache.ossie.converter.ConverterConstants.Level;
import org.apache.ossie.converter.pipeline.PipelineStep;
import org.apache.ossie.exception.ConversionException;
import java.util.*;

import org.apache.ossie.util.MappingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bidirectional handler for mapping metrics between Ossie and Salesforce formats.
 *
 * <p>Supports both conversion directions:
 * <ul>
 *   <li>Ossie → Salesforce: unwrap expression from dialects structure</li>
 *   <li>Salesforce → Ossie: wrap expression in dialects structure</li>
 * </ul>
 *
 */
public class MetricMappingHandler implements PipelineStep {

    private static final Logger logger = LoggerFactory.getLogger(MetricMappingHandler.class);

    private static final String METRICS = "metrics";
    private static final String SEMANTIC_CALCULATED_MEASUREMENTS = "semanticCalculatedMeasurements";

    private final ConversionDirection direction;
    private final CustomExtensionHandler customExtensionHandler;

    public MetricMappingHandler(ConversionDirection direction, CustomExtensionHandler customExtensionHandler) {
        this.direction = direction;
        this.customExtensionHandler = customExtensionHandler;
    }

    @Override
    public void execute(Map<String, Object> sourceData, Map<String, Object> outputData, Map<String, String> mappings) {
        logger.debug("Mapping metrics in {} direction", direction);
        if (direction == ConversionDirection.OSSIE_TO_SALESFORCE) {
            mapOssieToSalesforce(sourceData, outputData, mappings);
        } else {
            mapSalesforceToOssie(sourceData, outputData, mappings);
        }
    }

    /**
     * Maps Ossie metrics to Salesforce semanticCalculatedMeasurements.
     */
    private void mapOssieToSalesforce(
            Map<String, Object> sourceData, Map<String, Object> outputData, Map<String, String> mappings) {

        List<Object> ossieMetrics = getList(sourceData, METRICS);
        if (ossieMetrics == null) {
            return;
        }

        // Filter mappings to get only metric-related entries
        Map<String, String> metricMappings = MappingUtils.filterMappingsByPrefix(mappings, METRICS);

        Map<String, Object> mappedData = GenericMappingEngine.applyMappings(sourceData, metricMappings);
        metricMappings.keySet().forEach(mappings::remove);

        outputData.putAll(mappedData);

        // Restore before unwrapExpressions(): restore only fills absent keys, so exact
        // Salesforce values (e.g. dataType Currency) must land before derived ones.
        customExtensionHandler.restoreCustomExtensionsAtLevel(outputData, sourceData, Level.METRICS);

        List<Object> sfMetrics = getList(outputData, SEMANTIC_CALCULATED_MEASUREMENTS);
        if (sfMetrics != null) {
            unwrapExpressions(ossieMetrics, sfMetrics);
            applyDefaults(sfMetrics);
        }
    }

    /**
     * Applies default values for required Salesforce calculated measurement properties.
     * Used when converting Ossie → Salesforce.
     */
    private void applyDefaults(List<Object> sfMetrics) {
        for (Object sfMetricObj : sfMetrics) {
            Map<String, Object> sfMetric = asMap(sfMetricObj);
            if (!sfMetric.containsKey(LABEL) && sfMetric.containsKey(API_NAME)) {
                sfMetric.put(LABEL, getString(sfMetric, API_NAME));
            }
        }
    }

    /**
     * Maps Salesforce semanticCalculatedMeasurements to Ossie metrics.
     */
    private void mapSalesforceToOssie(
            Map<String, Object> sourceData, Map<String, Object> outputData, Map<String, String> mappings) {

        List<Object> sfMetrics = getList(sourceData, SEMANTIC_CALCULATED_MEASUREMENTS);
        if (sfMetrics == null) {
            return;
        }

        Map<String, String> metricMappings =
                MappingUtils.filterMappingsByPrefix(mappings, SEMANTIC_CALCULATED_MEASUREMENTS);

        Set<String> allHandledProps = metricMappings.isEmpty()? new HashSet<>() : MappingUtils.extractHandledProperties(metricMappings);
        allHandledProps.add(EXPRESSION);

        Map<String, Object> mappedData = GenericMappingEngine.applyMappings(sourceData, metricMappings);
        metricMappings.keySet().forEach(mappings::remove);

        outputData.putAll(mappedData);

        List<Object> ossieMetrics = getList(outputData, METRICS);
        if (ossieMetrics != null) {
            wrapExpressions(sfMetrics, ossieMetrics);
        }

        // Store unmapped SF properties in custom_extensions
        customExtensionHandler.storeUnmappedProperties(outputData, sourceData, allHandledProps, Level.METRICS);

        // Cleanup: remove processed structural key
        sourceData.remove(SEMANTIC_CALCULATED_MEASUREMENTS);
    }


    /**
     * Unwraps expressions for Ossie→SF conversion, mirroring {@link #wrapExpressions}.
     *
     * <p>Picks the {@code TABLEAU}-dialect expression out of each Ossie metric's
     * {@code expression.dialects[]} and flattens it into the Salesforce metric's
     * {@code expression} string. {@code TABLEAU} is what Salesforce/Tableau CRM itself speaks;
     * an {@code ANSI_SQL} expression is not translated to Tableau syntax, since that is the scope
     * of #222's expression-language work, not this fix (see #403). A metric with no TABLEAU
     * dialect fails the conversion rather than exporting an untranslated expression (#399).
     */
    private void unwrapExpressions(List<Object> ossieMetrics, List<Object> sfMetrics) {
        for (int i = 0; i < ossieMetrics.size() && i < sfMetrics.size(); i++) {
            Map<String, Object> ossieMetric = asMap(ossieMetrics.get(i));
            Map<String, Object> sfMetric = asMap(sfMetrics.get(i));

            String expressionValue = extractExpression(ossieMetric, DIALECT_TABLEAU);
            if (expressionValue == null) {
                throw new ConversionException(
                        "Metric '" + getString(ossieMetric, NAME) + "' has no TABLEAU expression "
                                + "to export to Salesforce; add one to expression.dialects[] or "
                                + "remove the metric.");
            }
            sfMetric.put(EXPRESSION, expressionValue);

            String datatype = SalesforceDataTypeMapper.toSalesforce(getString(ossieMetric, OSSIE_DATATYPE));
            if (datatype != null) {
                // Exact Salesforce value restored from custom_extensions wins over the
                // mapped Ossie datatype (e.g. a restored "Currency" over a mapped "Decimal").
                sfMetric.putIfAbsent(DATA_TYPE, datatype);
            }
        }
    }

    /**
     * Finds the given dialect's expression string in an Ossie metric's
     * {@code expression.dialects[]}, or {@code null} when the metric has no expression or no
     * entry for that dialect.
     */
    private String extractExpression(Map<String, Object> ossieMetric, String dialect) {
        Map<String, Object> expression = getMap(ossieMetric, EXPRESSION);
        if (expression == null) {
            return null;
        }
        List<Object> dialects = getList(expression, DIALECTS);
        if (dialects == null) {
            return null;
        }
        return streamMaps(dialects)
                .filter(d -> dialect.equals(getString(d, DIALECT)))
                .map(d -> getString(d, EXPRESSION))
                .findFirst()
                .orElse(null);
    }

    /**
     * Wraps expressions for SF→Ossie conversion.
     */
    private void wrapExpressions(List<Object> sfMetrics, List<Object> ossieMetrics) {
        for (int i = 0; i < sfMetrics.size() && i < ossieMetrics.size(); i++) {
            Map<String, Object> sfMetric = asMap(sfMetrics.get(i));
            Map<String, Object> ossieMetric = asMap(ossieMetrics.get(i));

            // Get expression from SF metric
            String expressionValue = getString(sfMetric, EXPRESSION);
            if (expressionValue != null) {
                // Wrap in Ossie dialect structure
                ossieMetric.put(EXPRESSION, wrapExpression(expressionValue));
            }

            String datatype = SalesforceDataTypeMapper.toOssie(getString(sfMetric, DATA_TYPE));
            if (datatype != null) {
                ossieMetric.put(OSSIE_DATATYPE, datatype);
            }
        }
    }

    /**
     * Wraps a simple expression string in Ossie's expression.dialects structure.
     * Tags expressions with TABLEAU dialect as they come from Salesforce (Tableau CRM).
     */
    private Map<String, Object> wrapExpression(String expressionValue) {
        Map<String, Object> dialect = new LinkedHashMap<>();
        dialect.put(DIALECT, DIALECT_TABLEAU);
        dialect.put(EXPRESSION, expressionValue);

        List<Object> dialects = new ArrayList<>();
        dialects.add(dialect);

        Map<String, Object> expression = new LinkedHashMap<>();
        expression.put(DIALECTS, dialects);

        return expression;
    }

}
