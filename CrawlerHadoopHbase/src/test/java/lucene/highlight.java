package lucene;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.highlight.*;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.StringReader;

/**
 * 搜索工具类
 * User: Winter Lau
 * Date: 13-1-10
 * Time: 上午11:54
 */
public class highlight {

    private final static Log log = LogFactory.getLog(highlight.class);
    private final static IKAnalyzer analyzer = new IKAnalyzer();
    private final static Formatter highlighter_formatter = new SimpleHTMLFormatter("<span class=\"highlight\">","</span>");
    /**
     * 对一段文本执行语法高亮处理
     * @param text 要处理高亮的文本
     * @param key 高亮的关键字
     * @return 返回格式化后的HTML文本
     */
    public static String highlight(String text, String key) {
        if(StringUtils.isBlank(key) || StringUtils.isBlank(text))
            return text;
        String result = null;
        try {
            key = QueryParser.escape(key.trim().toLowerCase());
            QueryScorer scorer = new QueryScorer(new TermQuery(new Term(null,QueryParser.escape(key))));
            Highlighter hig = new Highlighter(highlighter_formatter, scorer);
            TokenStream tokens = analyzer.tokenStream("content", new StringReader(text));
            result = hig.getBestFragment(tokens, text);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Unabled to hightlight text", e);
        }
        return (result != null)?result:text;
    }

    public static void main(String[] args) {
        String text = "Tomcat 是最好的 Java 应用服务器";
        System.out.println("RESULT:" + highlight.highlight(text, "Tomcat"));
    }

}