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
package org.apache.nutch.scoring.similarity.cosine;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.similarity.SimilarityModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CosineSimilarity implements SimilarityModel{

  private Configuration conf;
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public float setURLScoreAfterParsing(Text url, Content content, Parse parse) {
    float score = 1;

    try {
      if(!Model.isModelCreated){
        Model.createModel(conf);
      }
      
      int[] ngramArr = Model.retrieveNgrams(conf);
      int mingram = ngramArr[0];
      int maxgram = ngramArr[1];
      //extract keyword and description
      String metatags = parse.getData().getParseMeta().get("metatag.keyword");
      String metaDescription = " " + parse.getData().getParseMeta().get("metatag.description")+ " ";
      String doc = parse.getText()+metaDescription+metatags;
      DocVector docVector = Model.createDocVector(doc, mingram, maxgram);      
      //create title vector
      String title = parse.getData().getTitle();      
      DocVector titleVector = Model.createDocVector(title, mingram, maxgram);
      //create url vector      
      String url_str = url.toString().replace("/", " ").replace(".", " ");
      DocVector urlVector = Model.createDocVector(url_str, mingram, maxgram);
      
      //field length norm, plus 1 smoothing
      if(docVector!=null && titleVector!=null && urlVector!=null){
        int a = 100;
        float title_w = (float) (a/Math.sqrt(titleVector.termFreqVector.size() + 1));
        float url_w = (float) (a/Math.sqrt(urlVector.termFreqVector.size() + 1));
        float doc_w = (float) (a/Math.sqrt(docVector.termFreqVector.size() + 1));
        
        //the cosineSimilarity function has been changed (denominator removed)
        score = title_w*Model.computeCosineSimilarity(titleVector) + 
                url_w*Model.computeCosineSimilarity(urlVector) +
                doc_w*Model.computeCosineSimilarity(docVector);
        LOG.info("Setting score of {} to {}",url, score);
      }
      else {
        throw new Exception("Could not create DocVector from parsed text");
      }
    } catch (Exception e) {
      LOG.error("Error creating Cosine Model, setting scores of urls to 1 : {}", StringUtils.stringifyException(e));
    }
    return score;
  }

  @Override
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl, ParseData parseData,
      Collection<Entry<Text, CrawlDatum>> targets, CrawlDatum adjust,
      int allCount) {
    float score = Float.parseFloat(parseData.getContentMeta().get(Nutch.SCORE_KEY));
    
    int[] ngramArr = Model.retrieveNgrams(conf);
    int mingram = ngramArr[0];
    int maxgram = ngramArr[1];
    Outlink[] outlinks = parseData.getOutlinks();
    for (Entry<Text, CrawlDatum> target : targets) {
      String toUrl = target.getKey().toString();
      String toUrl_str = toUrl.replace("/", " ").replace(".", " ");
      String to_anchor = "";     
      for(Outlink out:outlinks)
      {
        if(out.getToUrl().equals(toUrl))
          to_anchor = out.getAnchor();         
      }
      DocVector urlVector = Model.createDocVector(toUrl_str, mingram, maxgram);
      DocVector anchorVector = Model.createDocVector(to_anchor, mingram, maxgram);
      float ep = 0.00000000001f;
      if(Model.computeCosineSimilarity(urlVector) < ep && 
          Model.computeCosineSimilarity(anchorVector) < ep)
        score = score/3;

      target.getValue().setScore(score);
    }
    return adjust;
  }

}