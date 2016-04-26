/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package de.qaware.chronix.converter

import de.qaware.chronix.ChronixClient
import de.qaware.chronix.solr.client.ChronixSolrStorage
import de.qaware.chronix.timeseries.MetricTimeSeries
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.HttpSolrClient
import spock.lang.Ignore
import spock.lang.Specification

import java.util.stream.Collectors

import static de.qaware.chronix.converter.KassiopeiaSimpleDefaults.GROUP_BY
import static de.qaware.chronix.converter.KassiopeiaSimpleDefaults.REDUCE

/**
 * Integration test - ignored on travis
 * @author f.lautenschlager
 */
class KassiopeiaSimpleIntegrationTest extends Specification {

    @Ignore
    def "test integration"() {
        given:
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/chronix")
        ChronixClient<MetricTimeSeries, SolrClient, SolrQuery> chronix = new ChronixClient<>(new KassiopeiaSimpleConverter(), new ChronixSolrStorage<>(200, GROUP_BY, REDUCE));

        when:
        SolrQuery query = new SolrQuery("metric:*Load*")
        query.addFilterQuery("ag=max;min")
        def result = chronix.stream(solr, query).collect(Collectors.toList())

        result.size()

        then:
        noExceptionThrown()
    }

}