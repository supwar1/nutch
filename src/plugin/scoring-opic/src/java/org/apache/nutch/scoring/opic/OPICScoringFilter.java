/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.scoring.opic;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

// Slf4j Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;

/**
 * This plugin implements a variant of an Online Page Importance Computation
 * (OPIC) score, described in this paper: <a
 * href="http://www2003.org/cdrom/papers/refereed/p007/p7-abiteboul.html"/>
 * Abiteboul, Serge and Preda, Mihai and Cobena, Gregory (2003), Adaptive
 * On-Line Page Importance Computation </a>.
 * 
 * @author Andrzej Bialecki
 */
public class OPICScoringFilter implements ScoringFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Configuration conf;
  private float scoreInjected;
  private float scorePower;
  private float internalScoreFactor;
  private float externalScoreFactor;
  private boolean countFiltered;

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    scorePower = conf.getFloat("indexer.score.power", 0.5f);
    internalScoreFactor = conf.getFloat("db.score.link.internal", 1.0f);
    externalScoreFactor = conf.getFloat("db.score.link.external", 1.0f);
    countFiltered = conf.getBoolean("db.score.count.filtered", false);
  }

  public void injectedScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
    // the datum.setScore(scoreInjected) is done in injector class
  }

  /**
   * Set to 0.0f (unknown value) - inlink contributions will bring it to a
   * correct level. Newly discovered pages have at least one inlink.
   */
  public void initialScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
    // invoked before distributeToOutlinks in the parse step. Another invoke happens  during updateDB
    //datum.setOpicScore(0.0f);
    datum.setOpicScore(1.0f);
  }

  /** Use {@link CrawlDatum#getScore()}. */
  public float generatorSortValue(Text url, CrawlDatum datum, float initSort)
      throws ScoringFilterException {
    return datum.getOpicScore() * initSort;
  }

  /** Increase the score by a sum of inlinked scores. */
  public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum,
      List<CrawlDatum> inlinked) throws ScoringFilterException {
    float adjust = 0.0f;
    for (int i = 0; i < inlinked.size(); i++) {
      CrawlDatum linked = inlinked.get(i);
      adjust += linked.getOpicScore();
    }
    if (old == null)
      old = datum;
    datum.setOpicScore(old.getOpicScore() + adjust);
  }

  /** Store a float value of CrawlDatum.getScore() under Fetcher.SCORE_KEY. */
  public void passScoreBeforeParsing(Text url, CrawlDatum datum, Content content) {
    content.getMetadata().set(Nutch.OPIC_SCORE_KEY, "" + datum.getOpicScore());
  }

  /** Copy the value from Content metadata under Fetcher.SCORE_KEY to parseData. */
  public void passScoreAfterParsing(Text url, Content content, Parse parse) {
    parse.getData().getContentMeta()
        .set(Nutch.OPIC_SCORE_KEY, content.getMetadata().get(Nutch.OPIC_SCORE_KEY));
  }

  /**
   * Get a float value from Fetcher.SCORE_KEY, divide it by the number of
   * outlinks and apply.
   */
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl,
      ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets,
      CrawlDatum adjust, int allCount) throws ScoringFilterException {
    //float score = scoreInjected;
    float score = 1.0f;
    String scoreString = parseData.getContentMeta().get(Nutch.OPIC_SCORE_KEY);
    if (scoreString != null) {
      try {
        score = Float.parseFloat(scoreString);
      } catch (Exception e) {
        LOG.error("Error: ", e);
      }
    }
    int validCount = targets.size();
    if (countFiltered) {
      score /= allCount;
    } else {
      if (validCount == 0) {
        // no outlinks to distribute score, so just return adjust
        return adjust;
      }
      score /= validCount;
    }
    // internal and external score factor
    float internalScore = score * internalScoreFactor;
    float externalScore = score * externalScoreFactor;
    for (Entry<Text, CrawlDatum> target : targets) {
      try {
        String toHost = new URL(target.getKey().toString()).getHost();
        String fromHost = new URL(fromUrl.toString()).getHost();
        if (toHost.equalsIgnoreCase(fromHost)) {
          target.getValue().setOpicScore(internalScore);
        } else {
          target.getValue().setOpicScore(externalScore);
        }
      } catch (MalformedURLException e) {
        LOG.error("Error: ", e);
        target.getValue().setOpicScore(externalScore);
      }
    }
    // XXX (ab) no adjustment? I think this is contrary to the algorithm descr.
    // XXX in the paper, where page "loses" its score if it's distributed to
    // XXX linked pages...
    return adjust;
  }

  /** Dampen the boost value by scorePower. */
  public float indexerScore(Text url, NutchDocument doc, CrawlDatum dbDatum,
      CrawlDatum fetchDatum, Parse parse, Inlinks inlinks, float initScore)
      throws ScoringFilterException {
    //return (float) Math.pow(dbDatum.getOpicScore(), scorePower) * initScore;
    return initScore;
  }
}
