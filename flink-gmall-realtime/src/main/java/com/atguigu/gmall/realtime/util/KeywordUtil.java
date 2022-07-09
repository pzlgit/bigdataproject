package com.atguigu.gmall.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * IK分词器工具类
 *
 * @author pangzl
 * @create 2022-07-09 16:38
 */
public class KeywordUtil {

    public static List<String> analyze(String text) {
        List<String> keywordList = new ArrayList<>();
        StringReader reader = new StringReader(text);
        // true：使用智能分词策略（合并数词和量词，对分词结果进行歧义判断）
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
        try {
            Lexeme lexeme = null;
            while ((lexeme = ikSegmenter.next()) != null) {
                String keyword = lexeme.getLexemeText();
                keywordList.add(keyword);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return keywordList;
    }

    public static void main(String[] args) {
        List<String> list = analyze("Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待");
        System.out.println(list);
    }

}
