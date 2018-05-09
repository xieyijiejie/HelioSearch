package org.apache.lucene.analysis.ngram;

/*
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

import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.apache.lucene.util.Version;

/**
 * Simple tests to ensure the NGram filter factories are working.
 */
public class TestNGramFilters extends BaseTokenStreamFactoryTestCase {
  /**
   * Test NGramTokenizerFactory
   */

  public void testNGramTokenizer() throws Exception {
    String s="\"在药品零售行业陷入低谷之际，外资正在趁机抄底。南都记者近日独家获悉，广州市内多家“百济新特药房”的招牌开始更换为“康德乐大药房”。而这一门店招牌更换的背后，则是世界50强上市企业-美国第二大医疗保健服务商康德乐(C ar-dinal H ealth)对中国药品分销业的再度抄底。     南都记者多方采访确认，康德乐已经在10月份悄然与广州百济新特药业连锁有限公司完成股权交割，前者由此成为百济新特药业的大股东。 　　康德乐低调“入穗” 　　来自国内药店数据库中康资讯的数据显示，目前，百济新特连锁药业年销售规模已达2亿元左右。广州百济新特药业连锁有限公司原总经理夏语11月11日接受南都记者采访时，确认了公司被康德乐并购一事，但并未透露包括交易金额在内的更多合作细节。 　　尽管没有采芝林、大参林、金康、老百姓、海王星辰等知名度高，但这一总部设在广州的连锁药店，自2002年成立华南地区首家专科药房以来，已先后在广州、北京、上海、深圳、成都、杭州等城市建立了19家分店，成为全国最大的专科医药连锁企业。 　　南都记者从业内获悉，康德乐为了快速切入广东市场，早在去年底就已经与百济新特药业开始接洽。双方于今年10月正式达成协议，完成股权交割。据接近百济新特药业的人士介绍，康德乐入主后，将基本保留百济新特药的原班团队，夏语将出任康德乐大药房的副总经理。但在总体决策及对外沟通方面则由康德乐(中国)公司全面负责。 　　由于负责百济新特药业项目的康德乐中国高级副总裁目前正在美国开会，南都记者未能联系到该位副总裁置评，不过她表示将在月底对此项合作作出回应。不过从百济新特药业方面传出的消息，康德乐此番并购百济新特药房后，将整合双方现有的29家零售药店，同时将加速网络拓展，争取年底前完成除西藏等少数地区外的20个以上省市区的布点。 　　全面布局的野心 　　“国际医药分销巨头进来，对国内药房来说是一个威胁。由于目前药品分销已经对外资开放，康德乐控股百济新特药业已经没有政策方面的障碍。”国药控股高级研究员干荣富在接受南都记者采访时如是分析。在干荣富看来，依照新医改的要求，药品零售行业连锁占比需要从目前的大约1/3提升至2/3，这一要求的出炉，令国际药品分销巨头看到了机会。 　　据了解，在此番控股百济新特药业前，康德乐2011年曾以4 .7亿美元收购此前在华浸淫多年的外资公司永裕医药，由此拿下了永裕医药在中国市场的9大物流中心，330个城市的销售渠道，以及由6700多家批发商、4 .9万家医院、12 .3万家零售药店和1250家疾病控制中心组成的销售联盟。 　　作为一家主要从事进口药分销业务的专业分销企业，永裕医药的原有业务为康德乐计划在华推广的D T P模式，提供了极好的基础。康德乐中国此前曾披露的三期规划显示，其目标是要在中国内地布局100个城市，实现30亿元的销售额。而所谓的D T P模式，即主要是专业处方药的外销业务模式，该模式的目标消费者主要针对由厂家推荐的手持医生处方的患者用户。 　　在一 些 业内 分 析人 士 看来，此番康德乐入主百济新特药业，与D T P模式存在一定关系。百济新特药房在国内多个城市的布点都靠近当地三甲医院，且也是靠D T P模式，通过销售高价专科药发家。此外，百济新特药业近年来发展的O 2O业务，也被认为是吸引康德乐的因素之一。 　　外资巨头的中国战场 　　尽管身为美国第二大医疗保健服务商的康德乐于1993年进入上海市场提供进口药的分销管理服务，2003年成为第一个被商务部和原国家食品药品监督管理局授予医药分销许可证的外资公司，但其随后的布局却被当时欧洲最大的医药分销商联合博姿所赶超。截至目前联合博姿已经与广药集团合资建有广州医药―――这一华南地区举足轻重的医药分销区域龙头。此外，联合博姿还入股南京医药，目前正在对南京医药实施业务模式改造。 　　2012年6月美国最大的零售药店连锁沃尔格林公司收购联合博姿45%股份后，则进一步加大了对广州医药和南京医药的重视，来自广药集团的消息就显示，双方高层近期互访频频。 　　发力较晚的康德乐此番入驻百济新特药业，在业内看来，其实是国际医药分销巨头在华布局战再起的一个信号。依照业内的消息，康德乐整编百济新特药业，还将在国内推广医院药房托管业务，以大力提升在华的业绩。其广东省内首个药房托管项目已经在今年落户深圳。 　　不过，南都记者同时也注意到，广东省卫生计生委副主任廖新波对药房托管尚存有不同看法，其今年9月发表在《南方日报》的文章指“药房托管是‘披着羊皮的狼’”。 　　南都记者 马建忠";
char[] chars = s.toCharArray();
    //    String s = "test";
    Reader reader = new StringReader(s);
//    Reader reader = new StringReader("test");
//    TokenStream stream = tokenizerFactory("NGram").create(reader);
    TokenStream stream = tokenizerFactory("NGram",
            "minGramSize", "1",
            "maxGramSize", "1").create(reader);

    CharTermAttribute cta = stream.addAttribute(CharTermAttribute.class);
    stream.reset();

    while(stream.incrementToken()){
      System.out.print("[" + cta + "]");
    }
    stream.end();
    stream.close();
//    assertTokenStreamContents(stream,
//        new String[] { "t", "te", "e", "es", "s", "st", "t" });
  }

  /**
   * Test NGramTokenizerFactory with min and max gram options
   */
  public void testNGramTokenizer2() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = tokenizerFactory("NGram",
        "minGramSize", "2",
        "maxGramSize", "3").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "te", "tes", "es", "est", "st" });
  }

  /**
   * Test the NGramFilterFactory
   */
  public void testNGramFilter() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("NGram").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "t", "te", "e", "es", "s", "st", "t" });
  }

  /**
   * Test the NGramFilterFactory with min and max gram options
   */
  public void testNGramFilter2() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("NGram",
        "minGramSize", "2",
        "maxGramSize", "3").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "te", "tes", "es", "est", "st" });
  }

  /**
   * Test EdgeNGramTokenizerFactory
   */
  public void testEdgeNGramTokenizer() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = tokenizerFactory("EdgeNGram").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "t" });
  }

  /**
   * Test EdgeNGramTokenizerFactory with min and max gram size
   */
  public void testEdgeNGramTokenizer2() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = tokenizerFactory("EdgeNGram",
        "minGramSize", "1",
        "maxGramSize", "2").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "t", "te" });
  }

  /**
   * Test EdgeNGramTokenizerFactory with side option
   */
  public void testEdgeNGramTokenizer3() throws Exception {
    Reader reader = new StringReader("ready");
    TokenStream stream = tokenizerFactory("EdgeNGram", Version.LUCENE_4_3,
        "side", "back").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "y" });
  }

  /**
   * Test EdgeNGramFilterFactory
   */
  public void testEdgeNGramFilter() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("EdgeNGram").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "t" });
  }

  /**
   * Test EdgeNGramFilterFactory with min and max gram size
   */
  public void testEdgeNGramFilter2() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("EdgeNGram",
        "minGramSize", "1",
        "maxGramSize", "2").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "t", "te" });
  }

  /**
   * Test EdgeNGramFilterFactory with side option
   */
  public void testEdgeNGramFilter3() throws Exception {
    Reader reader = new StringReader("ready");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("EdgeNGram", Version.LUCENE_4_3,
        "side", "back").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "y" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenizerFactory("NGram", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
    
    try {
      tokenizerFactory("EdgeNGram", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
    
    try {
      tokenFilterFactory("NGram", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
    
    try {
      tokenFilterFactory("EdgeNGram", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
